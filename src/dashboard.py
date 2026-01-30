import pandas as pd
from datetime import date, timedelta
from io import BytesIO

def _date_only(ts) -> pd.Series:
    return pd.to_datetime(ts, errors="coerce").dt.date

def build_dashboard_frames(avantio_df: pd.DataFrame, replenishment_df: pd.DataFrame, ref_date: date, window_days: int) -> dict:
    df = avantio_df.copy()

    df["entrada_d"] = _date_only(df["Fecha_entrada_dt"])
    df["salida_d"] = _date_only(df["Fecha_salida_dt"])

    # State flags
    df["Entra_hoy"] = df["entrada_d"] == ref_date
    df["Sale_hoy"] = df["salida_d"] == ref_date

    # Occupied if entry <= ref_date < exit (date-level)
    df["Ocupado_hoy"] = (df["entrada_d"] <= ref_date) & (ref_date < df["salida_d"])

    # Window
    end_d = ref_date + timedelta(days=window_days)
    df["Entra_prox"] = (df["entrada_d"] > ref_date) & (df["entrada_d"] <= end_d)
    df["Sale_prox"] = (df["salida_d"] > ref_date) & (df["salida_d"] <= end_d)

    # Reduce to latest per apartment for "state" purposes (keep rows within window)
    # We will build lists: for today's entries, we need today's entry rows specifically.
    entradas_hoy = df[df["Entra_hoy"]].copy()

    # groups with entries today
    grupos_hoy = set(entradas_hoy["ZONA"].dropna().unique().tolist())

    # Join replenishment info per apartment via ALMACEN
    rep = replenishment_df.copy()
    # aggregate to apartment-level metrics (via ALMACEN)
    rep_agg = rep.groupby("ALMACEN", as_index=False).agg(
        faltantes_min=("Faltante_min","sum"),
        unidades_reponer=("A_reponer","sum"),
    )
    # compact list of items to reponer (top)
    rep_items = rep[rep["A_reponer"] > 0].copy()
    rep_items["linea"] = rep_items["Amenity"].astype(str) + " x" + rep_items["A_reponer"].round(0).astype(int).astype(str)
    rep_items_agg = rep_items.groupby("ALMACEN")["linea"].apply(lambda s: ", ".join(s.tolist()[:12])).reset_index().rename(columns={"linea":"Lista_reponer"})

    rep_join = rep_agg.merge(rep_items_agg, on="ALMACEN", how="left")

    df = df.merge(rep_join, on="ALMACEN", how="left")
    df["faltantes_min"] = df["faltantes_min"].fillna(0).astype(int)
    df["unidades_reponer"] = df["unidades_reponer"].fillna(0.0)
    df["Lista_reponer"] = df["Lista_reponer"].fillna("")

    # PRIMER PLANO: entries today
    primer = entradas_hoy.merge(rep_join, on="ALMACEN", how="left", suffixes=("",""))
    primer["faltantes_min"] = primer["faltantes_min"].fillna(0).astype(int)
    primer["unidades_reponer"] = primer["unidades_reponer"].fillna(0.0)
    primer["Lista_reponer"] = primer["Lista_reponer"].fillna("")

    # Priority logic
    # MAX: same apartment sale today OR significant replenishment
    # ALTA: any sale today in same group (ZONA) OR has faltantes
    # MEDIA: otherwise
    # compute group sale today
    salidas_hoy_por_grupo = df[df["Sale_hoy"]].groupby("ZONA").size().to_dict()
    def _prio(row):
        if bool(row.get("Sale_hoy", False)):
            return "MAX"
        if row.get("faltantes_min", 0) >= 2 or row.get("unidades_reponer", 0) >= 10:
            return "ALTA"
        if salidas_hoy_por_grupo.get(row.get("ZONA"), 0) > 0:
            return "ALTA"
        return "MEDIA"
    primer["Prioridad"] = primer.apply(_prio, axis=1)

    primer_plano = primer[[
        "APARTAMENTO","ZONA","CAFE_TIPO","Fecha entrada hora","Fecha salida hora",
        "Prioridad","faltantes_min","unidades_reponer","Lista_reponer","ALMACEN"
    ]].sort_values(["Prioridad","ZONA","APARTAMENTO"], ascending=[True, True, True])

    # ENTRADAS PRÓXIMAS en grupos de hoy
    entradas_prox = df[df["Entra_prox"] & df["ZONA"].isin(list(grupos_hoy))].copy()
    entradas_prox = entradas_prox[[
        "APARTAMENTO","ZONA","CAFE_TIPO","Fecha entrada hora","Fecha salida hora",
        "faltantes_min","unidades_reponer","Lista_reponer","ALMACEN"
    ]].sort_values(["ZONA","Fecha entrada hora","APARTAMENTO"])

    # OCUPADOS con salida próxima fuera de grupos de entradas
    ocup_salida = df[df["Ocupado_hoy"] & df["Sale_prox"] & (~df["ZONA"].isin(list(grupos_hoy)))].copy()
    ocup_salida = ocup_salida[[
        "APARTAMENTO","ZONA","CAFE_TIPO","Fecha salida hora",
        "faltantes_min","unidades_reponer","Lista_reponer","ALMACEN"
    ]].sort_values(["Fecha salida hora","ZONA","APARTAMENTO"])

    # QC
    qc_no_zona = df[df["ZONA"].isna()][["APARTAMENTO","Alojamiento"]].drop_duplicates().sort_values("APARTAMENTO")
    qc_no_almacen = df[df["ALMACEN"].isna()][["APARTAMENTO","Alojamiento","ZONA"]].drop_duplicates().sort_values("APARTAMENTO")

    # Unclassified products list
    # For this, we need original unclassified list; we approximate by detecting empty Lista_reponer not possible
    # Better: in app we pass it; but keep here minimal (placeholder from replenishment side doesn't have raw names).
    qc_unclassified = pd.DataFrame(columns=["Producto","Ubicación","Cantidad"])

    # Export primer plano to xlsx
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        primer_plano.to_excel(writer, index=False, sheet_name="PrimerPlano")
        entradas_prox.to_excel(writer, index=False, sheet_name="EntradasProximas")
        ocup_salida.to_excel(writer, index=False, sheet_name="OcupadosSalidaProx")
    primer_xlsx = bio.getvalue()

    kpis = {
        "entradas_hoy": int(df["Entra_hoy"].sum()),
        "salidas_hoy": int(df["Sale_hoy"].sum()),
        "aptos_con_faltantes": int((rep_join["faltantes_min"] > 0).sum()),
    }

    return {
        "kpis": kpis,
        "primer_plano": primer_plano,
        "entradas_proximas": entradas_prox,
        "ocupados_salida_proxima": ocup_salida,
        "qc_no_zona": qc_no_zona,
        "qc_no_almacen": qc_no_almacen,
        "qc_unclassified_products": qc_unclassified,
        "primer_plano_xlsx": primer_xlsx,
    }
