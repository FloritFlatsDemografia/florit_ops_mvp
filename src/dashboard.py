import pandas as pd
from datetime import date, timedelta
from io import BytesIO
import math


COFFEE_AMENITIES = {
    "Café molido",
    "Cápsulas Nespresso",
    "Cápsulas Tassimo",
    "Cápsulas Dolce Gusto",
}


def _safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, float) and math.isnan(x):
            return ""
    except Exception:
        pass
    return str(x)


def _allowed_coffee_amenity(cafe_tipo) -> str | None:
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
    return None


def build_dashboard_frames(
    avantio_df: pd.DataFrame,
    replenishment_df: pd.DataFrame,
    ref_date: date,
    window_days: int,
    unclassified_products: pd.DataFrame,
) -> dict:

    df = avantio_df.copy()

    # =========================================================
    # ✅ FUENTE ÚNICA DE VERDAD PARA FECHAS: columnas visibles
    # =========================================================
    entrada_dt = pd.to_datetime(df.get("Fecha entrada hora"), errors="coerce", dayfirst=True)
    salida_dt = pd.to_datetime(df.get("Fecha salida hora"), errors="coerce", dayfirst=True)

    df["entrada_d"] = entrada_dt.dt.date
    df["salida_d"] = salida_dt.dt.date

    # Estados
    df["Entra_hoy"] = df["entrada_d"] == ref_date
    df["Sale_hoy"] = df["salida_d"] == ref_date
    df["Ocupado_hoy"] = (df["entrada_d"] <= ref_date) & (ref_date < df["salida_d"])

    start_prox = ref_date + timedelta(days=1)                 # mañana
    end_prox = ref_date + timedelta(days=window_days)         # fin ventana

    # =========================================================
    # ✅ Bloque 2: SOLO futuras, sin incluir hoy
    # =========================================================
    df["Entra_prox"] = (df["entrada_d"] >= start_prox) & (df["entrada_d"] <= end_prox)
    df["Sale_prox"] = (df["salida_d"] > ref_date) & (df["salida_d"] <= end_prox)

    # ---------------------------------------------------------
    # Reposición (filtrar café por CAFE_TIPO)
    # ---------------------------------------------------------
    rep = replenishment_df.copy()

    if "ALMACEN" in df.columns and "ALMACEN" in rep.columns:
        rep = rep.merge(
            df[["ALMACEN", "CAFE_TIPO"]].drop_duplicates(),
            on="ALMACEN",
            how="left",
        )

    def keep_row(r):
        amen = r.get("Amenity")
        if amen not in COFFEE_AMENITIES:
            return True
        allowed = _allowed_coffee_amenity(r.get("CAFE_TIPO"))
        if allowed is None:
            return True
        return amen == allowed

    rep = rep[rep.apply(keep_row, axis=1)].copy()

    rep_items = rep[rep.get("A_reponer", 0) > 0].copy()
    if not rep_items.empty:
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
    else:
        rep_items_agg = pd.DataFrame(columns=["ALMACEN", "Lista_reponer"])

    if "ALMACEN" in df.columns:
        df = df.merge(rep_items_agg, on="ALMACEN", how="left")
    else:
        df["Lista_reponer"] = ""

    df["Lista_reponer"] = df["Lista_reponer"].fillna("")

    # ---------------------------------------------------------
    # 1) Entradas HOY (limpio)
    # ---------------------------------------------------------
    entradas_hoy = df[df["Entra_hoy"]].copy()
    entradas_hoy = entradas_hoy[
        ["APARTAMENTO", "ZONA", "CAFE_TIPO", "Fecha entrada hora", "Fecha salida hora", "Lista_reponer"]
    ].sort_values(["ZONA", "APARTAMENTO"])

    # ---------------------------------------------------------
    # 2) Entradas PRÓXIMAS (desde mañana) (limpio)
    # ---------------------------------------------------------
    entradas_proximas = df[df["Entra_prox"]].copy()
    entradas_proximas = entradas_proximas[
        ["APARTAMENTO", "ZONA", "CAFE_TIPO", "Fecha entrada hora", "Fecha salida hora", "Lista_reponer"]
    ].sort_values(["Fecha entrada hora", "ZONA", "APARTAMENTO"])

    # ---------------------------------------------------------
    # 3) Ocupados con salida próxima (limpio)
    # ---------------------------------------------------------
    ocupados_salida = df[df["Ocupado_hoy"] & df["Sale_prox"]].copy()
    ocupados_salida = ocupados_salida[
        ["APARTAMENTO", "ZONA", "CAFE_TIPO", "Fecha salida hora", "Lista_reponer"]
    ].sort_values(["Fecha salida hora", "ZONA", "APARTAMENTO"])

    # KPIs
    aptos_con_faltantes = int(df[df["Lista_reponer"].astype(str).str.strip().ne("")]["APARTAMENTO"].nunique())

    # Excel
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        entradas_hoy.to_excel(writer, index=False, sheet_name="EntradasHoy")
        entradas_proximas.to_excel(writer, index=False, sheet_name="EntradasProximas")
        ocupados_salida.to_excel(writer, index=False, sheet_name="OcupadosSalidaProx")

    return {
        "kpis": {
            "entradas_hoy": int(df["Entra_hoy"].sum()),
            "salidas_hoy": int(df["Sale_hoy"].sum()),
            "aptos_con_faltantes": aptos_con_faltantes,
        },
        "entradas_hoy": entradas_hoy,
        "entradas_proximas": entradas_proximas,
        "ocupados_salida_proxima": ocupados_salida,
        "excel_all": bio.getvalue(),
    }
