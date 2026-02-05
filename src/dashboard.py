import pandas as pd
from io import BytesIO
import xlsxwriter


STATE_PRIORITY = {
    "ENTRADA+SALIDA": 0,
    "ENTRADA": 1,
    "SALIDA": 2,
    "OCUPADO": 3,
    "VACIO": 4,
}


COFFEE_KEYS = {"cafe_tassimo", "cafe_nespresso", "cafe_molido", "cafe_dolcegusto"}


def _safe_dt(s):
    return pd.to_datetime(s, errors="coerce")


def _coffee_allowed_keys(cafe_tipo: str) -> set[str]:
    t = str(cafe_tipo or "").strip().lower()
    if "tassimo" in t:
        return {"cafe_tassimo"}
    if "nespresso" in t:
        return {"cafe_nespresso"}
    if "molido" in t:
        return {"cafe_molido"}
    if "dolce" in t:
        return {"cafe_dolcegusto"}
    return set()  # si no está definido, mejor no meter café


def _build_replenishment_list_per_apt(apt_df: pd.DataFrame, rep_df: pd.DataFrame) -> pd.DataFrame:
    """
    apt_df: APARTAMENTO, ALMACEN, CAFE_TIPO
    rep_df:  ALMACEN, AmenityKey, Amenity, A_reponer, Faltante_min, ...
    """
    if rep_df is None or rep_df.empty:
        apt_df = apt_df.copy()
        apt_df["Lista_reponer"] = ""
        return apt_df

    rep = rep_df.copy()
    rep = rep[rep["A_reponer"] > 0].copy()

    # join por ALMACEN (cada apt tiene su almacén)
    tmp = apt_df[["APARTAMENTO", "ALMACEN", "CAFE_TIPO"]].merge(rep, on="ALMACEN", how="left")

    # filtra café por tipo del apartamento
    def keep_row(r):
        k = r.get("AmenityKey")
        if pd.isna(k):
            return False
        if k in COFFEE_KEYS:
            allowed = _coffee_allowed_keys(r.get("CAFE_TIPO"))
            return k in allowed
        return True

    tmp = tmp[tmp.apply(keep_row, axis=1)].copy()

    # Texto final por apt
    tmp["item"] = tmp.apply(
        lambda r: f"{r.get('Amenity','')} x{int(round(float(r.get('A_reponer',0))))}",
        axis=1,
    )

    agg = tmp.groupby("APARTAMENTO", as_index=False)["item"].apply(lambda s: ", ".join([x for x in s.tolist() if x and x.strip()]))
    agg = agg.rename(columns={"item": "Lista_reponer"})

    out = apt_df.merge(agg, on="APARTAMENTO", how="left")
    out["Lista_reponer"] = out["Lista_reponer"].fillna("")
    return out


def build_dashboard_frames(
    avantio_df: pd.DataFrame,
    replenishment_df: pd.DataFrame,
    unclassified_products: pd.DataFrame | None = None,
    period_start=None,
    period_days: int = 2,
):
    df = avantio_df.copy()

    # Necesitamos: APARTAMENTO, ALMACEN, ZONA, CAFE_TIPO, Fecha entrada hora, Fecha salida hora
    # En tu parser suelen venir: "Fecha entrada hora" / "Fecha salida hora"
    if "Fecha entrada hora" not in df.columns or "Fecha salida hora" not in df.columns:
        # fallback por si vienen como "Fecha entrada" y "Fecha salida"
        for alt in ["Fecha entrada", "Entrada", "Check-in"]:
            if alt in df.columns and "Fecha entrada hora" not in df.columns:
                df = df.rename(columns={alt: "Fecha entrada hora"})
        for alt in ["Fecha salida", "Salida", "Check-out"]:
            if alt in df.columns and "Fecha salida hora" not in df.columns:
                df = df.rename(columns={alt: "Fecha salida hora"})

    df["in_dt"] = _safe_dt(df["Fecha entrada hora"])
    df["out_dt"] = _safe_dt(df["Fecha salida hora"])

    # Periodo
    start = pd.Timestamp(period_start).normalize()
    days = int(period_days)
    date_list = [start + pd.Timedelta(days=i) for i in range(days)]
    end = (start + pd.Timedelta(days=days - 1)).normalize()

    # Base apartamentos únicos
    base_cols = ["APARTAMENTO", "ZONA", "CAFE_TIPO", "ALMACEN"]
    base = df[base_cols].dropna(subset=["APARTAMENTO"]).drop_duplicates().copy()
    base["APARTAMENTO"] = base["APARTAMENTO"].astype(str).str.strip()
    base["ZONA"] = base["ZONA"].astype(str).str.strip()

    # Lista reponer por apt (según ALMACEN + café)
    base = _build_replenishment_list_per_apt(base, replenishment_df)

    # Precalcula “próxima entrada” por apt (próximo check-in posterior a cada día)
    # (lo recalculamos por día para que sea exacto)
    oper_rows = []

    for d in date_list:
        day_start = d
        day_end = d + pd.Timedelta(days=1)

        # Reservas relevantes para el día (solapan)
        # overlap: in < end && out > start
        day_res = df[(df["in_dt"] < day_end) & (df["out_dt"] > day_start)].copy()

        # entradas/salidas exactas
        in_today = df[df["in_dt"].dt.normalize() == day_start][["APARTAMENTO", "in_dt"]].copy()
        out_today = df[df["out_dt"].dt.normalize() == day_start][["APARTAMENTO", "out_dt"]].copy()

        in_today = in_today.sort_values("in_dt").drop_duplicates("APARTAMENTO")
        out_today = out_today.sort_values("out_dt").drop_duplicates("APARTAMENTO")

        occ = day_res[["APARTAMENTO"]].drop_duplicates()
        occ["OCUPA"] = True

        # empieza con base y asigna estado
        day_table = base.copy()
        day_table["Día"] = day_start.date()

        day_table = day_table.merge(occ, on="APARTAMENTO", how="left")
        day_table["OCUPA"] = day_table["OCUPA"].fillna(False)

        day_table = day_table.merge(in_today, on="APARTAMENTO", how="left")
        day_table = day_table.merge(out_today, on="APARTAMENTO", how="left")

        def compute_state(r):
            has_in = pd.notna(r.get("in_dt"))
            has_out = pd.notna(r.get("out_dt"))
            if has_in and has_out:
                return "ENTRADA+SALIDA"
            if has_in:
                return "ENTRADA"
            if has_out:
                return "SALIDA"
            if bool(r.get("OCUPA")):
                return "OCUPADO"
            return "VACIO"

        day_table["Estado"] = day_table.apply(compute_state, axis=1)
        day_table["__prio"] = day_table["Estado"].map(lambda x: STATE_PRIORITY.get(x, 99))

        day_table["Entrada hora"] = day_table["in_dt"].dt.strftime("%Y-%m-%d %H:%M:%S")
        day_table["Salida hora"] = day_table["out_dt"].dt.strftime("%Y-%m-%d %H:%M:%S")
        day_table["Entrada hora"] = day_table["Entrada hora"].fillna("")
        day_table["Salida hora"] = day_table["Salida hora"].fillna("")

        # próxima entrada futura (posterior al día)
        future_in = df[df["in_dt"] > day_end][["APARTAMENTO", "in_dt"]].copy()
        future_in = future_in.sort_values("in_dt").drop_duplicates("APARTAMENTO")
        future_in["Próxima Entrada"] = future_in["in_dt"].dt.date
        future_in = future_in[["APARTAMENTO", "Próxima Entrada"]]

        day_table = day_table.merge(future_in, on="APARTAMENTO", how="left")

        oper_rows.append(day_table)

    operativa = pd.concat(oper_rows, ignore_index=True)

    # KPIs del “día foco” (primer día del periodo)
    foco = start.date()
    foco_df = operativa[operativa["Día"] == foco]
    kpis = {
        "entradas_dia": int((foco_df["Estado"] == "ENTRADA").sum()),
        "salidas_dia": int((foco_df["Estado"] == "SALIDA").sum()),
        "turnovers_dia": int((foco_df["Estado"] == "ENTRADA+SALIDA").sum()),
        "ocupados_dia": int((foco_df["Estado"] == "OCUPADO").sum()),
        "vacios_dia": int((foco_df["Estado"] == "VACIO").sum()),
    }

    # Excel export
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        operativa.to_excel(writer, sheet_name="Operativa", index=False)
        if replenishment_df is not None and not replenishment_df.empty:
            replenishment_df.to_excel(writer, sheet_name="Reposicion_por_almacen", index=False)
        if unclassified_products is not None and not unclassified_products.empty:
            unclassified_products.to_excel(writer, sheet_name="Sin_clasificar", index=False)

        wb = writer.book
        for sh in writer.sheets.values():
            sh.freeze_panes(1, 0)

    excel_bytes = output.getvalue()

    return {
        "kpis": kpis,
        "operativa": operativa,
        "period_start": start.date(),
        "period_end": end.date(),
        "excel_all": excel_bytes,
        "excel_filename": f"Florit_OPS_Operativa_{start.date()}_{end.date()}.xlsx",
    }
