import pandas as pd
from io import BytesIO

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


def _build_replenishment_lists_per_apt(apt_df: pd.DataFrame, rep_df: pd.DataFrame) -> pd.DataFrame:
    """
    apt_df: APARTAMENTO, ALMACEN, CAFE_TIPO (y opcionalmente ZONA)
    rep_df:  ALMACEN, AmenityKey, Amenity, Cantidad, Minimo, Maximo, Faltan_para_min, A_reponer, Bajo_minimo
    Devuelve apt_df con:
      - Lista_reponer (para llegar a máximo)
      - Urgente_minimo (solo lo que está bajo mínimo, cantidad para llegar al mínimo)
      - Tiene_urgente (bool)
    """
    out = apt_df.copy()
    out["Lista_reponer"] = ""
    out["Urgente_minimo"] = ""
    out["Tiene_urgente"] = False

    if rep_df is None or rep_df.empty:
        return out

    rep = rep_df.copy()

    # columnas mínimas esperadas
    for c in ["ALMACEN", "AmenityKey"]:
        if c not in rep.columns:
            return out  # sin esto no podemos cruzar

    if "Amenity" not in rep.columns:
        rep["Amenity"] = rep["AmenityKey"]

    for c in ["A_reponer", "Faltan_para_min"]:
        if c not in rep.columns:
            rep[c] = 0

    rep["A_reponer"] = pd.to_numeric(rep["A_reponer"], errors="coerce").fillna(0)
    rep["Faltan_para_min"] = pd.to_numeric(rep["Faltan_para_min"], errors="coerce").fillna(0)

    # Join por ALMACEN
    tmp = out[["APARTAMENTO", "ALMACEN", "CAFE_TIPO"]].merge(rep, on="ALMACEN", how="left")

    # Filtra café por tipo del apartamento
    def keep_row(r):
        k = r.get("AmenityKey")
        if pd.isna(k) or k is None:
            return False
        if k in COFFEE_KEYS:
            allowed = _coffee_allowed_keys(r.get("CAFE_TIPO"))
            return k in allowed
        return True

    tmp = tmp[tmp.apply(keep_row, axis=1)].copy()

    # -------- Lista para llegar a MAX --------
    to_max = tmp[tmp["A_reponer"] > 0].copy()
    if not to_max.empty:
        to_max["item"] = to_max.apply(
            lambda r: f"{r.get('Amenity','')} x{int(round(float(r.get('A_reponer',0))))}",
            axis=1,
        )
        agg_max = (
            to_max.groupby("APARTAMENTO")["item"]
            .agg(lambda s: ", ".join([x for x in s.tolist() if str(x).strip()]))
            .reset_index(name="Lista_reponer")
        )
        out = out.merge(agg_max, on="APARTAMENTO", how="left")
        out["Lista_reponer"] = out["Lista_reponer"].fillna("")

    # -------- Urgente (para llegar a MIN) --------
    to_min = tmp[tmp["Faltan_para_min"] > 0].copy()
    if not to_min.empty:
        to_min["item_min"] = to_min.apply(
            lambda r: f"{r.get('Amenity','')} x{int(round(float(r.get('Faltan_para_min',0))))}",
            axis=1,
        )
        agg_min = (
            to_min.groupby("APARTAMENTO")["item_min"]
            .agg(lambda s: ", ".join([x for x in s.tolist() if str(x).strip()]))
            .reset_index(name="Urgente_minimo")
        )
        out = out.merge(agg_min, on="APARTAMENTO", how="left")
        out["Urgente_minimo"] = out["Urgente_minimo"].fillna("")
        urgent_flag = (
            to_min.groupby("APARTAMENTO").size().reset_index(name="_n")
        )
        urgent_flag["Tiene_urgente"] = True
        urgent_flag = urgent_flag[["APARTAMENTO", "Tiene_urgente"]]
        out = out.merge(urgent_flag, on="APARTAMENTO", how="left")
        out["Tiene_urgente"] = out["Tiene_urgente"].fillna(False)

    return out


def build_dashboard_frames(
    avantio_df: pd.DataFrame,
    replenishment_df: pd.DataFrame,
    unclassified_products: pd.DataFrame | None = None,
    period_start=None,
    period_days: int = 2,
):
    df = avantio_df.copy()

    # Detecta columnas de fechas (tolerante)
    if "Fecha entrada hora" not in df.columns:
        for alt in ["Fecha entrada", "Entrada", "Check-in"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Fecha entrada hora"})
                break
    if "Fecha salida hora" not in df.columns:
        for alt in ["Fecha salida", "Salida", "Check-out"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Fecha salida hora"})
                break

    df["in_dt"] = _safe_dt(df.get("Fecha entrada hora"))
    df["out_dt"] = _safe_dt(df.get("Fecha salida hora"))

    # Periodo
    start = pd.Timestamp(period_start).normalize()
    days = int(period_days)
    date_list = [start + pd.Timedelta(days=i) for i in range(days)]
    end = (start + pd.Timedelta(days=days - 1)).normalize()

    # Base apartamentos únicos
    base_cols = [c for c in ["APARTAMENTO", "ZONA", "CAFE_TIPO", "ALMACEN"] if c in df.columns]
    base = df[base_cols].dropna(subset=["APARTAMENTO"]).drop_duplicates().copy()
    base["APARTAMENTO"] = base["APARTAMENTO"].astype(str).str.strip()
    if "ZONA" in base.columns:
        base["ZONA"] = base["ZONA"].astype(str).str.strip()

    # Listas de reposición por apt (según ALMACEN + café)
    base = _build_replenishment_lists_per_apt(base, replenishment_df)

    oper_rows = []
    for d in date_list:
        day_start = d.normalize()
        day_end = day_start + pd.Timedelta(days=1)

        # Solape reservas (ocupado)
        day_res = df[(df["in_dt"] < day_end) & (df["out_dt"] > day_start)].copy()

        in_today = df[df["in_dt"].dt.normalize() == day_start][["APARTAMENTO", "in_dt"]].copy()
        out_today = df[df["out_dt"].dt.normalize() == day_start][["APARTAMENTO", "out_dt"]].copy()
        in_today = in_today.sort_values("in_dt").drop_duplicates("APARTAMENTO")
        out_today = out_today.sort_values("out_dt").drop_duplicates("APARTAMENTO")

        occ = day_res[["APARTAMENTO"]].drop_duplicates()
        occ["OCUPA"] = True

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

        # Próxima entrada futura (posterior al día)
        future_in = df[df["in_dt"] > day_end][["APARTAMENTO", "in_dt"]].copy()
        future_in = future_in.sort_values("in_dt").drop_duplicates("APARTAMENTO")
        future_in["Próxima Entrada"] = future_in["in_dt"].dt.date
        future_in = future_in[["APARTAMENTO", "Próxima Entrada"]]
        day_table = day_table.merge(future_in, on="APARTAMENTO", how="left")

        # Quitar columnas que pediste NO mostrar
        day_table = day_table.drop(columns=["OCUPA", "in_dt", "out_dt"], errors="ignore")

        oper_rows.append(day_table)

    operativa = pd.concat(oper_rows, ignore_index=True)

    # KPIs del “día foco”
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

        for sh in writer.sheets.values():
            sh.freeze_panes(1, 0)

    return {
        "kpis": kpis,
        "operativa": operativa,
        "period_start": start.date(),
        "period_end": end.date(),
        "excel_all": output.getvalue(),
        "excel_filename": f"Florit_OPS_Operativa_{start.date()}_{end.date()}.xlsx",
    }
