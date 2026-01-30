import pandas as pd
from datetime import date, timedelta
from io import BytesIO
import re
import math


def _date_only(ts) -> pd.Series:
    return pd.to_datetime(ts, errors="coerce").dt.date


COFFEE_AMENITIES = {
    "Café molido",
    "Cápsulas Nespresso",
    "Cápsulas Tassimo",
    "Cápsulas Dolce Gusto",
    "Cápsulas Senseo",
}


def _safe_str(x) -> str:
    # NaN puede venir como float
    if x is None:
        return ""
    try:
        if isinstance(x, float) and math.isnan(x):
            return ""
    except Exception:
        pass
    return str(x)


def _allowed_coffee_amenity(cafe_tipo) -> str | None:
    """
    Mapea el CAFE_TIPO del maestro a la familia de café que debe considerarse.
    """
    t = _safe_str(cafe_tipo).strip().lower()
    if t == "":
        return None

    if "molido" in t:
        return "Café molido"
    if "tassimo" in t:
        return "Cápsulas Tassimo"
    if "dolce" in t or "gusto" in t:
        return "Cápsulas Dolce Gusto"
    if "senseo" in t:
        return "Cápsulas Senseo"
    if "nespresso" in t or "colombia" in t:
        return "Cápsulas Nespresso"

    # fallback: si no sabemos, no filtramos (mejor verlo que esconderlo)
    return None


def build_dashboard_frames(
    avantio_df: pd.DataFrame,
    replenishment_df: pd.DataFrame,
    ref_date: date,
    window_days: int,
    unclassified_products: pd.DataFrame
) -> dict:

    df = avantio_df.copy()

    # --- Fechas ---
    df["entrada_d"] = _date_only(df["Fecha_entrada_dt"])
    df["salida_d"] = _date_only(df["Fecha_salida_dt"])

    # --- Estados ---
    df["Entra_hoy"] = df["entrada_d"] == ref_date
    df["Sale_hoy"] = df["salida_d"] == ref_date
    df["Ocupado_hoy"] = (df["entrada_d"] <= ref_date) & (ref_date < df["salida_d"])

    end_d = ref_date + timedelta(days=window_days)
    df["Entra_prox"] = (df["entrada_d"] > ref_date) & (df["entrada_d"] <= end_d)
    df["Sale_prox"] = (df["salida_d"] > ref_date) & (df["salida_d"] <= end_d)

    # =========================================================
    # REPOSICIÓN: filtrar café por apartamento (ALMACEN)
    # =========================================================
    rep = replenishment_df.copy()

    # Merge para conocer el CAFE_TIPO por ALMACEN
    rep = rep.merge(df[["ALMACEN", "CAFE_TIPO"]].drop_duplicates(), on="ALMACEN", how="left")

    def keep_row(r):
        amen = r.get("Amenity")
        if amen not in COFFEE_AMENITIES:
            return True
        allowed = _allowed_coffee_amenity(r.get("CAFE_TIPO"))
        if allowed is None:
            return True  # no filtramos si no sabemos
        return amen == allowed

    rep = rep[rep.apply(keep_row, axis=1)].copy()

    # Agregados por ALMACEN
    rep_agg = rep.groupby("ALMACEN", as_index=False).agg(
        faltantes_min=("Faltante_min", "sum"),
        unidades_reponer=("A_reponer", "sum"),
    )

    rep_items = rep[rep["A_reponer"] > 0].copy()
    rep_items["linea"] = (
        rep_items["Amenity"].astype(str)
        + " x"
        + rep_items["A_reponer"].round(0).astype(int).astype(str)
    )

    rep_items_agg = (
        rep_items.groupby("ALMACEN")["linea"]
        .apply(lambda s: ", ".join(s.tolist()[:30]))
        .reset_index()
        .rename(columns={"linea": "Lista_reponer"})
    )

    rep_join = rep_agg.merge(rep_items_agg, on="ALMACEN", how="left")

    df = df.merge(rep_join, on="ALMACEN", how="left")
    df["faltantes_min"] = df["faltantes_min"].fillna(0).astype(int)
    df["unidades_reponer"] = df["unidades_reponer"].fillna(0)
    df["Lista_reponer"] = df["Lista_reponer"].fillna("")

    # =========================================================
    # 0) PICKING HOY
    # =========================================================
    picking_hoy = df[df["unidades_reponer"] > 0].copy()

    def prioridad_picking(row):
        if row["Entra_hoy"]:
            return "1_ENTRA_HOY"
        if row["Entra_prox"]:
            return "2_ENTRA_PROX"
        if row["Sale_hoy"]:
            return "3_SALE_HOY"
        if row["Sale_prox"]:
            return "4_SALE_PROX"
        return "5_RESTO"

    if not picking_hoy.empty:
        picking_hoy["Prioridad"] = picking_hoy.apply(prioridad_picking, axis=1)
        picking_hoy = picking_hoy[
            [
                "APARTAMENTO", "ZONA", "CAFE_TIPO",
                "Fecha entrada hora", "Fecha salida hora",
                "Prioridad", "faltantes_min", "unidades_reponer",
                "Lista_reponer", "ALMACEN",
            ]
        ].sort_values(["Prioridad", "ZONA", "APARTAMENTO"])
    else:
        picking_hoy = picking_hoy.reindex(columns=[
            "APARTAMENTO", "ZONA", "CAFE_TIPO", "Fecha entrada hora", "Fecha salida hora",
            "Prioridad", "faltantes_min", "unidades_reponer", "Lista_reponer", "ALMACEN"
        ])

    # 1) Entradas HOY
    entradas_hoy = df[df["Entra_hoy"]].copy()
    if not entradas_hoy.empty:
        entradas_hoy = entradas_hoy[
            [
                "APARTAMENTO", "ZONA", "CAFE_TIPO",
                "Fecha entrada hora", "Fecha salida hora",
                "faltantes_min", "unidades_reponer", "Lista_reponer", "ALMACEN",
            ]
        ].sort_values(["faltantes_min", "unidades_reponer"], ascending=False)
    else:
        entradas_hoy = entradas_hoy.reindex(columns=[
            "APARTAMENTO", "ZONA", "CAFE_TIPO",
            "Fecha entrada hora", "Fecha salida hora",
            "faltantes_min", "unidades_reponer", "Lista_reponer", "ALMACEN"
        ])

    # 2) Entradas próximas
    entradas_proximas = df[df["Entra_prox"]].copy()
    entradas_proximas = entradas_proximas[
        [
            "APARTAMENTO", "ZONA", "CAFE_TIPO",
            "Fecha entrada hora", "Fecha salida hora",
            "faltantes_min", "unidades_reponer", "Lista_reponer", "ALMACEN",
        ]
    ].sort_values(["Fecha entrada hora", "ZONA", "APARTAMENTO"])

    # 3) Ocupados con salida próxima
    ocupados_salida = df[df["Ocupado_hoy"] & df["Sale_prox"]].copy()
    ocupados_salida = ocupados_salida[
        [
            "APARTAMENTO", "ZONA", "CAFE_TIPO",
            "Fecha salida hora",
            "faltantes_min", "unidades_reponer", "Lista_reponer", "ALMACEN",
        ]
    ].sort_values(["Fecha salida hora", "ZONA", "APARTAMENTO"])

    # QC
    qc_no_zona = df[df["ZONA"].isna()][["APARTAMENTO"]].drop_duplicates()
    qc_no_almacen = df[df["ALMACEN"].isna()][["APARTAMENTO", "ZONA"]].drop_duplicates()
    if unclassified_products is None or unclassified_products.empty:
        qc_unclassified = pd.DataFrame(columns=["ALMACEN", "Producto", "Cantidad"])
    else:
        qc_unclassified = unclassified_products.copy().sort_values(["ALMACEN", "Producto"])

    # Excel export
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        picking_hoy.to_excel(writer, index=False, sheet_name="PickingHoy")
        entradas_hoy.to_excel(writer, index=False, sheet_name="EntradasHoy")
        entradas_proximas.to_excel(writer, index=False, sheet_name="EntradasProximas")
        ocupados_salida.to_excel(writer, index=False, sheet_name="OcupadosSalidaProx")
        qc_no_zona.to_excel(writer, index=False, sheet_name="QC_SinZona")
        qc_no_almacen.to_excel(writer, index=False, sheet_name="QC_SinAlmacen")
        qc_unclassified.to_excel(writer, index=False, sheet_name="QC_NoClasificados")

    return {
        "kpis": {
            "entradas_hoy": int(df["Entra_hoy"].sum()),
            "salidas_hoy": int(df["Sale_hoy"].sum()),
            "aptos_con_faltantes": int((rep_join["faltantes_min"] > 0).sum()),
        },
        "picking_hoy": picking_hoy,
        "entradas_hoy": entradas_hoy,
        "entradas_proximas": entradas_proximas,
        "ocupados_salida_proxima": ocupados_salida,
        "qc_no_zona": qc_no_zona,
        "qc_no_almacen": qc_no_almacen,
        "qc_unclassified_products": qc_unclassified,
        "excel_all": bio.getvalue(),
    }
