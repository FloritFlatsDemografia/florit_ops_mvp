import pandas as pd
from io import BytesIO

STATE_PRIORITY = {
    "ENTRADA+SALIDA": 0,
    "ENTRADA": 1,
    "SALIDA": 2,
    "OCUPADO": 3,
    "VACIO": 4,
}

COFFEE_KEYS = {"cafe_tassimo", "cafe_nespresso", "cafe_molido", "cafe_dolcegusto", "cafe_senseo"}


def _safe_dt(s):
    # Robust: primero intento normal (ISO), si salen muchos NaT pruebo dayfirst (dd/mm).
    dt = pd.to_datetime(s, errors="coerce")
    try:
        if hasattr(dt, "isna") and dt.isna().mean() > 0.5:
            dt2 = pd.to_datetime(s, errors="coerce", dayfirst=True)
            if dt2.isna().sum() < dt.isna().sum():
                dt = dt2
    except Exception:
        pass
    return dt


def _coffee_allowed_keys(cafe_tipo: str) -> set[str]:
    t = str(cafe_tipo or "").strip().lower()
    if not t:
        return set()
    if "tassimo" in t:
        return {"cafe_tassimo"}
    if "nespresso" in t or "colombia" in t:
        return {"cafe_nespresso"}
    if "molido" in t:
        return {"cafe_molido"}
    if "senseo" in t:
        return {"cafe_senseo"}
    if "dolce" in t or "gusto" in t:
        return {"cafe_dolcegusto"}
    return set()


def _find_client_col(df: pd.DataFrame) -> str | None:
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    norm = {c: str(c).strip().lower() for c in cols}

    # exactos
    for target in ["cliente", "huesped", "huésped", "ocupante", "guest", "nombre"]:
        for c, n in norm.items():
            if n == target:
                return c

    # contiene
    for c, n in norm.items():
        if "cliente" in n:
            return c
    for c, n in norm.items():
        if "ocup" in n or "huesp" in n or "guest" in n:
            return c

    return None


def _build_replenishment_list_per_apt(apt_df: pd.DataFrame, rep_df: pd.DataFrame) -> pd.DataFrame:
    out = apt_df.copy()
    out["Lista_reponer"] = ""

    if rep_df is None or rep_df.empty:
        return out

    rep = rep_df.copy()
    if "ALMACEN" not in rep.columns or "AmenityKey" not in rep.columns or "A_reponer" not in rep.columns:
        return out

    rep["A_reponer"] = pd.to_numeric(rep["A_reponer"], errors="coerce").fillna(0)
    rep = rep[rep["A_reponer"] > 0].copy()
    if rep.empty:
        return out

    if "Amenity" not in rep.columns:
        rep["Amenity"] = rep["AmenityKey"].astype(str)

    tmp = out[["APARTAMENTO", "ALMACEN", "CAFE_TIPO"]].merge(rep, on="ALMACEN", how="left")
    tmp = tmp.dropna(subset=["AmenityKey"]).copy()

    def keep_row(r):
        k = str(r.get("AmenityKey") or "")
        if not k:
            return False
        if k in COFFEE_KEYS:
            allowed = _coffee_allowed_keys(r.get("CAFE_TIPO"))
            return k in allowed
        return True

    tmp = tmp[tmp.apply(keep_row, axis=1)].copy()
    if tmp.empty:
        return out

    tmp["qty"] = pd.to_numeric(tmp["A_reponer"], errors="coerce").fillna(0).round(0).astype(int)
    tmp = tmp[tmp["qty"] > 0].copy()

    tmp["item"] = tmp.apply(lambda r: f"{r.get('Amenity','')} x{int(r['qty'])}", axis=1)

    agg = (
        tmp.groupby("APARTAMENTO")["item"]
        .agg(lambda s: ", ".join([x for x in s.tolist() if isinstance(x, str) and x.strip()])[:60])
        .reset_index()
        .rename(columns={"item": "Lista_reponer"})
    )

    out = out.drop(columns=["Lista_reponer"], errors="ignore").merge(agg, on="APARTAMENTO", how="left")
    out["Lista_reponer"] = out["Lista_reponer"].fillna("").astype(str)
    return out


def build_dashboard_frames(
    avantio_df: pd.DataFrame,
    replenishment_df: pd.DataFrame,
    unclassified_products: pd.DataFrame | None = None,
    period_start=None,
    period_days: int = 2,
) -> dict:
    df = avantio_df.copy()

    # Asegura columnas fecha
    if "Fecha entrada hora" not in df.columns or "Fecha salida hora" not in df.columns:
        for alt in ["Fecha entrada", "Entrada", "Check-in"]:
            if alt in df.columns and "Fecha entrada hora" not in df.columns:
                df = df.rename(columns={alt: "Fecha entrada hora"})
        for alt in ["Fecha salida", "Salida", "Check-out"]:
            if alt in df.columns and "Fecha salida hora" not in df.columns:
                df = df.rename(columns={alt: "Fecha salida hora"})

    df["in_dt"] = _safe_dt(df.get("Fecha entrada hora"))
    df["out_dt"] = _safe_dt(df.get("Fecha salida hora"))

    # Cliente
    client_col = _find_client_col(df)
    if client_col:
        df["CLIENTE"] = df[client_col].astype(str).str.strip()
        df.loc[df["CLIENTE"].str.lower().isin(["nan", "none"]), "CLIENTE"] = ""
    else:
        df["CLIENTE"] = ""

    # Periodo
    start = pd.Timestamp(period_start).normalize() if period_start is not None else pd.Timestamp.today().normalize()
    days = max(1, int(period_days))
    date_list = [start + pd.Timedelta(days=i) for i in range(days)]
    end = (start + pd.Timedelta(days=days - 1)).normalize()

    # Base apartments
    base_cols = ["APARTAMENTO", "ZONA", "CAFE_TIPO", "ALMACEN"]
    for c in base_cols:
        if c not in df.columns:
            df[c] = ""

    base = df[base_cols].dropna(subset=["APARTAMENTO"]).drop_duplicates().copy()
    base["APARTAMENTO"] = base["APARTAMENTO"].astype(str).str.strip()
    base["ZONA"] = base["ZONA"].astype(str).str.strip()
    base["CAFE_TIPO"] = base["CAFE_TIPO"].astype(str).str.strip()
    base["ALMACEN"] = base["ALMACEN"].astype(str).str.strip()

    base = _build_replenishment_list_per_apt(base, replenishment_df)

    oper_rows = []

    for d in date_list:
        day_start = d
        day_end = d + pd.Timedelta(days=1)

        day_res = df[(df["in_dt"] < day_end) & (df["out_dt"] > day_start)].copy()

        in_today = df[df["in_dt"].dt.normalize() == day_start][["APARTAMENTO", "in_dt", "CLIENTE"]].copy()
        out_today = df[df["out_dt"].dt.normalize() == day_start][["APARTAMENTO", "out_dt", "CLIENTE"]].copy()

        in_today = in_today.sort_values("in_dt").drop_duplicates("APARTAMENTO")
        out_today = out_today.sort_values("out_dt").drop_duplicates("APARTAMENTO")

        occ = day_res[["APARTAMENTO"]].drop_duplicates()
        occ["OCUPA"] = True

        day_table = base.copy()
        day_table["Día"] = day_start.date()

        day_table = day_table.merge(occ, on="APARTAMENTO", how="left")
        day_table["OCUPA"] = day_table["OCUPA"].fillna(False)

        day_table = day_table.merge(in_today.rename(columns={"CLIENTE": "CLIENTE_IN"}), on="APARTAMENTO", how="left")
        day_table = day_table.merge(out_today.rename(columns={"CLIENTE": "CLIENTE_OUT"}), on="APARTAMENTO", how="left")

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

        def pick_cliente(r):
            if pd.notna(r.get("CLIENTE_IN")) and str(r.get("CLIENTE_IN")).strip():
                return str(r.get("CLIENTE_IN")).strip()
            if pd.notna(r.get("CLIENTE_OUT")) and str(r.get("CLIENTE_OUT")).strip():
                return str(r.get("CLIENTE_OUT")).strip()
            return ""

        day_table["Cliente"] = day_table.apply(pick_cliente, axis=1)

        future_in = df[df["in_dt"] > day_end][["APARTAMENTO", "in_dt"]].copy()
        future_in = future_in.sort_values("in_dt").drop_duplicates("APARTAMENTO")
        future_in["Próxima Entrada"] = future_in["in_dt"].dt.date
        future_in = future_in[["APARTAMENTO", "Próxima Entrada"]]
        day_table = day_table.merge(future_in, on="APARTAMENTO", how="left")

        # Output limpio (sin in_dt/out_dt/ocupa/horas)
        keep_cols = [
            "Día",
            "ZONA",
            "APARTAMENTO",
            "Cliente",
            "Estado",
            "CAFE_TIPO",
            "Lista_reponer",
            "Próxima Entrada",
            "__prio",
        ]
        for c in keep_cols:
            if c not in day_table.columns:
                day_table[c] = ""
        day_table = day_table[keep_cols].copy()

        oper_rows.append(day_table)

    operativa = pd.concat(oper_rows, ignore_index=True)

    foco = start.date()
    foco_df = operativa[operativa["Día"] == foco]
    kpis = {
        "entradas_dia": int((foco_df["Estado"] == "ENTRADA").sum()),
        "salidas_dia": int((foco_df["Estado"] == "SALIDA").sum()),
        "turnovers_dia": int((foco_df["Estado"] == "ENTRADA+SALIDA").sum()),
        "ocupados_dia": int((foco_df["Estado"] == "OCUPADO").sum()),
        "vacios_dia": int((foco_df["Estado"] == "VACIO").sum()),
    }

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
