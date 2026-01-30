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

    # --- Fechas (solo día) ---
    df["entrada_d"] = _date_only(df["Fecha_entrada_dt"])
    df["salida_d"] = _date_only(df["Fecha_salida_dt"])

    # --- Estados diarios ---
    df["Entra_hoy"] = df["entrada_d"] == ref_date
    df["Sale_hoy"] = df["salida_d"] == ref_date
    df["Ocupado_hoy"] = (df["entrada_d"] <= ref_date) & (ref_date < df["salida_d"])

    # --- Ventana próximos días (desde mañana hasta ref_date+N) ---
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
    # 0) PICKING HOY – TODO LO QUE HAY QUE REPONER (OPERATIVO)
    # =========================================================
    picking_hoy = df[df["unidades_reponer"] > 0].copy()

    def prioridad_picking(row):
        # Prioridad logística:
        # 1) Entradas hoy
        # 2) Entradas próximas
        # 3) Salidas hoy
        # 4) Salidas próximas
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
    else:
        picking_hoy = picking_hoy.reindex(columns=[
            "APARTAMENTO", "ZONA", "CAFE_TIPO", "Fecha entrada hora", "Fecha salida hora",
            "Prioridad", "faltantes_min", "unidades_reponer", "Lista_reponer", "ALMACEN"
        ])

    # =========================================================
    # 1) PRIMER PLANO – ENTRADAS HOY (solo los que entran hoy)
    # =========================================================
    primer_plano = df[df["Entra_hoy"]].copy()

    def prioridad_entrada(row):
        if row["faltantes_min"] > 0:
            return "1_FALTANTE_MIN"
        if row["unidades_reponer"] > 0:
            return "2_REPONER"
        return "3_OK"

    if not primer_plano.empty:
        primer_plano["Prioridad"] = primer_plano.apply(prioridad_entrada, axis=1)
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
    else:
        primer_plano = primer_plano.reindex(columns=[
            "APARTAMENTO", "ZONA", "CAFE_TIPO", "Fecha entrada hora", "Fecha salida hora",
            "Prioridad", "faltantes_min", "unidades_reponer", "Lista_reponer", "ALMACEN"
        ])

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
        picking_hoy.to_excel(writer, index=False, sheet_name="PickingHoy")
        primer_plano.to_excel(writer, index=False, sheet_name="EntradasHoy")
        entradas_proximas.to_excel(writer, index=False, sheet_name="EntradasProximas")
        ocupados_salida.to_excel(writer, index=False, sheet_name="OcupadosSalidaProx")

    return {
        "kpis": {
            "entradas_hoy": int(df["Entra_hoy"].sum()),
            "salidas_hoy": int(df["Sale_hoy"].sum()),
            "aptos_con_faltantes": int((rep_join["faltantes_min"] > 0).sum()),
        },
        "picking_hoy": picking_hoy,
        "primer_plano": primer_plano,
        "entradas_proximas": entradas_proximas,
        "ocupados_salida_proxima": ocupados_salida,
        "qc_no_zona": qc_no_zona,
        "qc_no_almacen": qc_no_almacen,
        "qc_unclassified_products": qc_unclassified,
        "primer_plano_xlsx": bio.getvalue(),
    }
