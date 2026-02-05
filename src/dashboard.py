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

# AmenityKey que tratamos como “café” (se filtra por CAFE_TIPO)
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


def _as_int(x) -> int:
    try:
        return int(round(float(x)))
    except Exception:
        return 0


def _join_items(series: pd.Series, limit: int = 80) -> str:
    xs = []
    for v in series.tolist():
        if isinstance(v, str) and v.strip():
            xs.append(v.strip())
        if len(xs) >= limit:
            break
    return ", ".join(xs)


def _build_replenishment_lists_per_apt(apt_df: pd.DataFrame, rep_df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye 2 columnas por APARTAMENTO:
      - Lista_reponer  -> para llegar a MAXIMO (A_reponer)
      - Bajo_minimo    -> lo urgente para llegar a MINIMO (Faltan_para_min)

    apt_df: APARTAMENTO, ALMACEN, CAFE_TIPO (y lo que quieras conservar)
    rep_df:  ALMACEN, AmenityKey, Amenity, A_reponer, Faltan_para_min, ...
    """
    out = apt_df.copy()
    out["Lista_reponer"] = ""
    out["Bajo_minimo"] = ""

    if rep_df is None or rep_df.empty:
        return out

    rep = rep_df.copy()

    # Columnas esperadas (robusto)
    if "ALMACEN" not in rep.columns:
        rep["ALMACEN"] = ""

    if "AmenityKey" not in rep.columns:
        rep["AmenityKey"] = ""

    if "Amenity" not in rep.columns:
        rep["Amenity"] = ""

    if "A_reponer" not in rep.columns:
        rep["A_reponer"] = 0

    if "Faltan_para_min" not in rep.columns:
        # compat por si viniera con otros nombres
        for alt in ["Faltante_min", "Faltan_min", "Faltante_minimo"]:
            if alt in rep.columns:
                rep["Faltan_para_min"] = rep[alt]
                break
        else:
            rep["Faltan_para_min"] = 0

    rep["A_reponer"] = pd.to_numeric(rep["A_reponer"], errors="coerce").fillna(0)
    rep["Faltan_para_min"] = pd.to_numeric(rep["Faltan_para_min"], errors="coerce").fillna(0)

    # join por ALMACEN (cada apt tiene su almacén)
    tmp = out[["APARTAMENTO", "ALMACEN", "CAFE_TIPO"]].merge(rep, on="ALMACEN", how="left")

    # filtra café por tipo del apartamento
    def keep_row(r):
        k = r.get("AmenityKey")
        if pd.isna(k) or str(k).strip() == "":
            return False

        k = str(k).strip()

        if k in COFFEE_KEYS:
            allowed = _coffee_allowed_keys(r.get("CAFE_TIPO"))
            return k in allowed

        return True

    tmp = tmp[tmp.apply(keep_row, axis=1)].copy()

    # -------- Lista_reponer (hasta máximo) --------
    tmp_max = tmp[tmp["A_reponer"] > 0].copy()
    if not tmp_max.empty:
        tmp_max["item"] = tmp_max.apply(
            lambda r: f"{r.get('Amenity','')} x{_as_int(r.get('A_reponer', 0))}",
            axis=1,
        )
        agg_max = (
            tmp_max.groupby("APARTAMENTO")["item"]
            .apply(_join_items)
            .reset_index(name="Lista_reponer")
        )
        out = out.merge(agg_max, on="APARTAMENTO", how="left")
        out["Lista_reponer"] = out["Lista_reponer"].fillna("")

    # -------- Bajo_minimo (urgente: hasta mínimo) --------
    tmp_min = tmp[tmp["Faltan_para_min"] > 0].copy()
    if not tmp_min.empty:
        tmp_min["item"] = tmp_min.apply(
            lambda r: f"{r.get('Amenity','')} x{_as_int(r.get('Faltan_para_min', 0))}",
            axis=1,
        )
        agg_min = (
            tmp_min.groupby("APARTAMENTO")["item"]
            .apply(_join_items)
            .reset_index(name="Bajo_minimo")
        )
        out = out.merge(agg_min, on="APARTAMENTO", how="left")
        out["Bajo_minimo"] = out["Bajo_minimo"].fillna("")

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
    if "Fecha entrada hora" not in df.columns or "Fecha salida hora" not in df.columns:
        # fallback por si vienen con otros nombres
        for alt in ["Fecha entrada", "Entrada", "Check-in"]:
            if alt in df.columns and "Fecha entrada hora" not in df.columns:
                df = df.rename(columns={alt: "Fecha entrada hora"})
        for alt in ["Fecha salida", "Salida", "Check-out"]:
            if alt in df.columns and "Fecha salida hora" not in df.columns:
                df = df.rename(columns={alt: "Fecha salida hora"})

    # Parse datetimes
    df["in_dt"] = _safe_dt(df.get("Fecha entrada hora"))
    df["out_dt"] = _safe_dt(df.get("Fecha salida hora"))

    # Periodo (robusto)
    if period_start is None:
        start = pd.Timestamp.today().normalize()
    else:
        start = pd.Timestamp(period_start).normalize()

    days = int(period_days) if period_days else 2
    days = max(1, days)

    date_list = [start + pd.Timedelta(days=i) for i in range(days)]
    end = (start + pd.Timedelta(days=days - 1)).normalize()

    # Base apartamentos únicos
    base_cols = ["APARTAMENTO", "ZONA", "CAFE_TIPO", "ALMACEN"]
    for c in base_cols:
        if c not in df.columns:
            df[c] = None

    base = df[base_cols].dropna(subset=["APARTAMENTO"]).drop_duplicates().copy()
    base["APARTAMENTO"] = base["APARTAMENTO"].astype(str).str.strip()
    base["ZONA"] = base["ZONA"].astype(str).str.strip()

    # Lista reponer (máximo) + Bajo_minimo (mínimo) por apt (según ALMACEN + café)
    base = _build_replenishment_lists_per_apt(base, replenishment_df)

    oper_rows = []

    for d in date_list:
        day_start = d
        day_end = d + pd.Timedelta(days=1)

        # reservas que solapan el día: in < end && out > start
        day_res = df[(df["in_dt"] < day_end) & (df["out_dt"] > day_start)].copy()

        # entradas / salidas exactas del día
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

        # próxima entrada futura (posterior al día)
        future_in = df[df["in_dt"] > day_end][["APARTAMENTO", "in_dt"]].copy()
        future_in = future_in.sort_values("in_dt").drop_duplicates("APARTAMENTO")
        future_in["Próxima Entrada"] = future_in["in_dt"].dt.date
        future_in = future_in[["APARTAMENTO", "Próxima Entrada"]]
        day_table = day_table.merge(future_in, on="APARTAMENTO", how="left")

        # Orden columnas (Bajo_minimo justo después de Lista_reponer)
        wanted = [
            "Día",
            "ZONA",
            "APARTAMENTO",
            "Estado",
            "Próxima Entrada",
            "Lista_reponer",
            "Bajo_minimo",
            "CAFE_TIPO",
            "__prio",
        ]
        keep = [c for c in wanted if c in day_table.columns] + [c for c in day_table.columns if c not in wanted]

        day_table = day_table[keep].copy()

        # Quitar columnas que NO quieres ver
        day_table = day_table.drop(
            columns=["OCUPA", "in_dt", "out_dt", "Entrada hora", "Salida hora"],
            errors="ignore",
        )

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
