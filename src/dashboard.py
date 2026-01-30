import pandas as pd
from datetime import date, timedelta
from io import BytesIO


def _date_only(ts) -> pd.Series:
    return pd.to_datetime(ts, errors="coerce").dt.date


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

    # --- Ventana próximos días ---
    end_d = ref_date + timedelta(days=window_days)
    df["Entra_prox"] = (df["entrada_d"] > ref_date) & (df["entrada_d"] <= end_d)
    df["Sale_prox"] = (df["salida_d"] > ref_date) & (df["salida_d"] <= end_d)

    # --- Reposición por ALMACEN ---
    rep = replenishment_df.copy()

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
        .apply(lambda s: ", ".join(s.tolist()[:20]))
        .reset_index()
        .rename(columns={"linea": "Lista_reponer"})
    )

    rep_join = rep_agg.merge(rep_items_agg, on="ALMACEN", how="left")

    df = df.merge(rep_join, on="ALMACEN", how="left")
    df["faltantes_min"] = df["faltantes_min"].fillna(0).astype(int)
    df["unidades_reponer"] = df["unidades_reponer"].fillna(0)
    df["Lista_reponer"] = df["Lista_reponer"].fillna("")

    # =========================================================
    # 1) PRIMER PLANO – PICKING DIARIO (TODO LO QUE FALTA)
    # =========================================================
    primer_plano = df[df["unidades_reponer"] > 0].copy()

    def prioridad(row):
        # orden operativo: hoy > próximas entradas > próximas salidas > resto
        if bool(row.get("Entra_hoy", False)):
            return "1_HOY"
        if bool(row.get("Entra_prox", False)):
            return "2_PROX_ENTRADA"
        if bool(row.get("Sale_prox", False)):
            return "3_PROX_SALIDA"
        return "4_RESTO"

    primer_plano["Prioridad"] = primer_plano.apply(prioridad, axis=1)

    primer_plano = primer_plano[
        [
            "APARTAMENTO",
            "ZONA",
            "CAFE_TIPO",
            "Fecha entrada hora",
            "Fecha salida hora",
            "Prioridad",
            "faltantes_min",
            "unidades_reponer",
            "Lista_reponer",
            "ALMACEN",
        ]
    ].sort_values(["Prioridad", "ZONA", "APARTAMENTO"])

    # =========================================================
    # 2) ENTRADAS PRÓXIMAS – SEGÚN VENTANA
    # =========================================================
    entradas_proximas = df[df["Entra_prox"]].copy()

    entradas_proximas = entradas_proximas[
        [
            "APARTAMENTO",
            "ZONA",
            "CAFE_TIPO",
            "Fecha entrada hora",
            "Fecha salida hora",
            "faltantes_min",
            "unidades_reponer",
            "Lista_reponer",
            "ALMACEN",
        ]
    ].sort_values(["Fecha entrada hora", "ZONA", "APARTAMENTO"])

    # =========================================================
    # 3) OCUPADOS con SALIDA PRÓXIMA – SEGÚN VENTANA
    # =========================================================
    ocupados_salida = df[df["Ocupado_hoy"] & df["Sale_prox"]].copy()

    ocupados_salida = ocupados_salida[
        [
            "APARTAMENTO",
            "ZONA",
            "CAFE_TIPO",
            "Fecha salida hora",
            "faltantes_min",
            "unidades_reponer",
            "Lista_reponer",
            "ALMACEN",
        ]
    ].sort_values(["Fecha salida hora", "ZONA", "APARTAMENTO"])

    # =========================================================
    # CONTROL DE CALIDAD
    # =========================================================
    qc_no_zona = df[df["ZONA"].isna()][["APARTAMENTO"]].drop_duplicates()
    qc_no_almacen = df[df["ALMACEN"].isna()][["APARTAMENTO", "ZONA"]].drop_duplicates()

    if unclassified_products is None or unclassified_products.empty:
        qc_unclassified = pd.DataFrame(columns=["ALMACEN", "Producto", "Cantidad"])
    else:
        qc_unclassified = unclassified_products.copy()
        qc_unclassified = qc_unclassified.sort_values(["ALMACEN", "Producto"])

    # =========================================================
    # EXPORT EXCEL
    # =========================================================
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        primer_plano.to_excel(writer, index=False, sheet_name="PrimerPlano")
        entradas_proximas.to_excel(writer, index=False, sheet_name="EntradasProximas")
        ocupados_salida.to_excel(writer, index=False, sheet_name="OcupadosSalidaProx")

    return {
        "kpis": {
            "entradas_hoy": int(df["Entra_hoy"].sum()),
            "salidas_hoy": int(df["Sale_hoy"].sum()),
            "aptos_con_faltantes": int((rep_join["faltantes_min"] > 0).sum()),
        },
        "primer_plano": primer_plano,
        "entradas_proximas": entradas_proximas,
        "ocupados_salida_proxima": ocupados_salida,
        "qc_no_zona": qc_no_zona,
        "qc_no_almacen": qc_no_almacen,
        "qc_unclassified_products": qc_unclassified,
        "primer_plano_xlsx": bio.getvalue(),
    }
