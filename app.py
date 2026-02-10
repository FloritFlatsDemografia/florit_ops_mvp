import streamlit as st
import pandas as pd
from zoneinfo import ZoneInfo
from urllib.parse import quote
import re

# ✅ tu repo tiene src/cleaning_last_report.py
from src.cleaning_last_report import build_last_report_view

ORIGIN_LAT = 39.45702028460933
ORIGIN_LNG = -0.38498336081567713


# =========================
# Helpers
# =========================
def _norm_apt_name(s: str) -> str:
    s = "" if s is None else str(s).strip().upper()
    s = re.sub(r"\s+", " ", s)
    # "APOLO 029" -> "APOLO 29"
    s = re.sub(r"\b0+(\d)", r"\1", s)
    return s


# =========================
# Google Maps helpers
# =========================
def _coord_str(lat, lng):
    try:
        return f"{float(lat):.8f},{float(lng):.8f}"
    except Exception:
        return None


def build_gmaps_directions_url(coords, travelmode="walking", return_to_base=False):
    clean = []
    seen = set()
    for c in coords:
        if isinstance(c, str) and "," in c and c not in seen:
            seen.add(c)
            clean.append(c)

    if not clean:
        return None

    origin = f"{ORIGIN_LAT:.8f},{ORIGIN_LNG:.8f}"

    if return_to_base:
        destination = origin
        waypoints = clean
    else:
        destination = clean[-1]
        waypoints = clean[:-1]

    wp = "|".join(waypoints)

    url = "https://www.google.com/maps/dir/?api=1"
    url += f"&origin={quote(origin)}"
    url += f"&destination={quote(destination)}"
    if wp:
        url += f"&waypoints={quote(wp)}"
    url += f"&travelmode={quote(travelmode)}"
    return url


def chunk_list(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


# =========================
# Styles
# =========================
def _style_operativa(df: pd.DataFrame):
    colors = {
        "ENTRADA+SALIDA": "#FFF3BF",
        "ENTRADA": "#D3F9D8",
        "SALIDA": "#FFE8CC",
        "OCUPADO": "#E7F5FF",
        "VACIO": "#F1F3F5",
    }

    def row_style(row):
        bg = colors.get(str(row.get("Estado", "")), "")
        if bg:
            return [f"background-color: {bg}"] * len(row)
        return [""] * len(row)

    return df.style.apply(row_style, axis=1)


# =========================
# Reposición parsing
# =========================
_ITEM_RX = re.compile(r"^\s*(.*?)\s*x\s*([0-9]+)\s*$", re.IGNORECASE)


def parse_lista_reponer(s: str):
    if s is None:
        return []
    txt = str(s).strip()
    if not txt:
        return []
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    out = []
    for p in parts:
        m = _ITEM_RX.match(p)
        if m:
            name = m.group(1).strip()
            qty = int(m.group(2))
            if name:
                out.append((name, qty))
        else:
            out.append((p, 1))
    return out


def build_sugerencia_df(operativa: pd.DataFrame, zonas_sel: list[str], include_completar: bool = False):
    df = operativa.copy()
    df = df[df["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA", "VACIO"])].copy()

    if zonas_sel:
        df = df[df["ZONA"].isin(zonas_sel)].copy()

    cols = ["Lista_reponer"]
    if include_completar and "Completar con" in df.columns:
        cols.append("Completar con")

    rows = []
    for _, r in df.iterrows():
        for col in cols:
            txt = r.get(col, "")
            if str(txt).strip() == "":
                continue
            items = parse_lista_reponer(txt)
            for prod, qty in items:
                rows.append(
                    {
                        "Día": r.get("Día"),
                        "ZONA": r.get("ZONA"),
                        "APARTAMENTO": r.get("APARTAMENTO"),
                        "Producto": prod,
                        "Cantidad": int(qty),
                        "Fuente": col,
                    }
                )

    items_df = pd.DataFrame(rows)
    if items_df.empty:
        totals_df = pd.DataFrame(columns=["Producto", "Total"])
        return items_df, totals_df

    totals_df = (
        items_df.groupby("Producto", as_index=False)["Cantidad"]
        .sum()
        .rename(columns={"Cantidad": "Total"})
        .sort_values(["Total", "Producto"], ascending=[False, True])
        .reset_index(drop=True)
    )

    items_df = items_df.sort_values(["ZONA", "APARTAMENTO", "Producto", "Fuente"]).reset_index(drop=True)
    return items_df, totals_df


# =========================
# Google Sheet helpers (día foco)
# =========================
def _agg_nonempty(series: pd.Series) -> str:
    vals = []
    for x in series.tolist():
        s = str(x).strip()
        if s and s.lower() not in {"nan", "none"}:
            vals.append(s)
    seen = set()
    out = []
    for v in vals:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return " | ".join(out)


def _extract_ops_from_sheet(sheet_df: pd.DataFrame, foco_date: pd.Timestamp) -> pd.DataFrame:
    if sheet_df is None or sheet_df.empty:
        return pd.DataFrame(columns=["APARTAMENTO", "Incidencias hoy", "Faltantes por entrada", "Reposiciones café"])

    df = sheet_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    def pick_col(name_candidates, idx_fallback):
        low = {str(c).strip().lower(): c for c in df.columns}
        for cand in name_candidates:
            k = cand.strip().lower()
            for lk, orig in low.items():
                if lk == k or k in lk:
                    return orig
        if df.shape[1] > idx_fallback:
            return df.columns[idx_fallback]
        return None

    c_ts = pick_col(["marca temporal", "timestamp", "fecha", "marca"], 0)  # A
    c_ap = pick_col(["apartamento"], 1)  # B
    c_inc = pick_col(["incidencias a realizar", "incidencias"], 6)  # G
    c_falt = pick_col(["faltantes por entrada", "faltantes"], 16)  # Q
    c_cafe = pick_col(["faltantes reposiciones café", "reposiciones café", "café"], 18)  # S

    if not c_ts or not c_ap:
        return pd.DataFrame(columns=["APARTAMENTO", "Incidencias hoy", "Faltantes por entrada", "Reposiciones café"])

    df["_ts"] = pd.to_datetime(df[c_ts], errors="coerce", dayfirst=True)
    df["_date"] = df["_ts"].dt.normalize()

    foco_norm = pd.Timestamp(foco_date).normalize()
    df = df[df["_date"] == foco_norm].copy()

    if df.empty:
        return pd.DataFrame(columns=["APARTAMENTO", "Incidencias hoy", "Faltantes por entrada", "Reposiciones café"])

    df["_AP"] = df[c_ap].astype(str).str.strip().str.upper().map(_norm_apt_name)
    df = df[df["_AP"].ne("") & df["_AP"].ne("NAN")].copy()

    out = df.groupby("_AP", as_index=False).agg(
        **{
            "Incidencias hoy": (c_inc, _agg_nonempty) if c_inc else ("_AP", lambda s: ""),
            "Faltantes por entrada": (c_falt, _agg_nonempty) if c_falt else ("_AP", lambda s: ""),
            "Reposiciones café": (c_cafe, _agg_nonempty) if c_cafe else ("_AP", lambda s: ""),
        }
    )
    out = out.rename(columns={"_AP": "APARTAMENTO"}).sort_values("APARTAMENTO").reset_index(drop=True)
    return out


# =========================
# Presenciales helpers
# =========================
PRESENCIALES = {_norm_apt_name(x) for x in ["APOLO 029", "APOLO 180", "APOLO 197", "SERRANOS"]}


def _get_presenciales_today_df(operativa_df: pd.DataFrame, tz_name: str = "Europe/Madrid") -> pd.DataFrame:
    if operativa_df is None or operativa_df.empty:
        return pd.DataFrame()

    tz = ZoneInfo(tz_name)
    today = pd.Timestamp.now(tz=tz).normalize().date()

    df = operativa_df.copy()
    if "Día" not in df.columns or "APARTAMENTO" not in df.columns:
        return pd.DataFrame()

    df["APARTAMENTO_NORM"] = df["APARTAMENTO"].astype(str).str.strip().str.upper().map(_norm_apt_name)

    df_today = df[df["Día"] == today].copy()
    df_today = df_today[df_today["APARTAMENTO_NORM"].isin(PRESENCIALES)].copy()

    # Solo ENTRADAS del día (si existe Estado)
    if "Estado" in df_today.columns:
        df_today = df_today[df_today["Estado"].astype(str).str.upper().str.contains("ENTRADA", na=False)].copy()

    return df_today


def main():
    from src.loaders import load_masters_repo
    from src.parsers import parse_avantio_entradas, parse_odoo_stock
    from src.normalize import normalize_products, summarize_replenishment
    from src.dashboard import build_dashboard_frames
    from src.gsheets import read_sheet_df

    st.set_page_config(page_title="Florit OPS – Operativa & Reposición", layout="wide")
    st.title("Florit OPS – Parte diario (Operativa + Reposición)")

    with st.expander("📌 Cómo usar", expanded=False):
        st.markdown(
            """
**Sube 2 archivos diarios:**
- **Avantio (Entradas)**: .xls / .xlsx / .csv / (xls HTML de Avantio)
- **Odoo (stock.quant)**: .xlsx / .csv

📌 Maestros en `data/` (GitHub):
- Zonas
- Apartamentos e Inventarios (ALMACEN + Localización)
- Café por apartamento
- Stock mínimo/máximo
"""
        )

    st.sidebar.header("Archivos diarios")
    avantio_file = st.sidebar.file_uploader("Avantio (Entradas)", type=["xls", "xlsx", "csv", "html"])
    odoo_file = st.sidebar.file_uploader("Odoo (stock.quant)", type=["xlsx", "csv"])

    with st.sidebar.expander("Avanzado (opcional)", expanded=True):
        st.subheader("Periodo operativo")
        period_start = st.date_input("Inicio", value=pd.Timestamp.today().date())
        period_days = st.number_input("Nº días", min_value=1, max_value=14, value=2, step=1)

        st.divider()
        st.subheader("Reposición")
        mode = st.radio(
            "Modo",
            ["Reponer hasta máximo", "URGENTE: solo bajo mínimo (pero reponiendo hasta máximo)"],
            index=0,
        )

        st.divider()
        st.subheader("Filtros")
        estados_sel = st.multiselect(
            "Filtrar estados",
            ["ENTRADA", "SALIDA", "ENTRADA+SALIDA", "OCUPADO", "VACIO"],
            default=["ENTRADA", "SALIDA", "ENTRADA+SALIDA", "OCUPADO", "VACIO"],
        )

        st.divider()
        st.subheader("Ruta (HOY + MAÑANA)")
        travelmode = st.selectbox("Modo", ["walking", "driving"], index=0)
        return_to_base = st.checkbox("Volver a Florit Flats al final", value=False)

    try:
        masters = load_masters_repo()
        st.sidebar.success("Maestros cargados ✅")
    except Exception as e:
        st.error("Fallo cargando maestros (data/).")
        st.exception(e)
        st.stop()

    zonas_all = (
        masters["zonas"]["ZONA"].dropna().astype(str).str.strip().unique().tolist()
        if "zonas" in masters and "ZONA" in masters["zonas"].columns
        else []
    )
    zonas_all = sorted([z for z in zonas_all if z and z.lower() not in ["nan", "none"]])
    zonas_sel = st.sidebar.multiselect("ZONAS (multiselección)", options=zonas_all, default=zonas_all)

    if not (avantio_file and odoo_file):
        st.info("Sube Avantio + Odoo para generar el parte operativo.")
        st.stop()

    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)
    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant.")
        st.stop()

    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()
    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")

    ap_map = masters["apt_almacen"].copy()
    need = {"APARTAMENTO", "ALMACEN"}
    if not need.issubset(set(ap_map.columns)):
        st.error(f"Maestro apt_almacen: faltan columnas {need}. Columnas: {list(ap_map.columns)}")
        st.stop()

    for c in ["LAT", "LNG"]:
        if c not in ap_map.columns:
            ap_map[c] = pd.NA

    if "Localizacion" in ap_map.columns:

        def _split_loc(x):
            s = str(x).strip()
            if "," in s:
                a, b = s.split(",", 1)
                return a.strip(), b.strip()
            return None, None

        miss = ap_map["LAT"].isna() | ap_map["LNG"].isna()
        if miss.any():
            loc_pairs = ap_map.loc[miss, "Localizacion"].apply(_split_loc)
            ap_map.loc[miss, "LAT"] = [p[0] for p in loc_pairs]
            ap_map.loc[miss, "LNG"] = [p[1] for p in loc_pairs]

    ap_map = (
        ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]]
        .dropna(subset=["APARTAMENTO", "ALMACEN"])
        .drop_duplicates()
    )
    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()

    avantio_df = avantio_df.merge(ap_map, on="APARTAMENTO", how="left")

    odoo_norm = normalize_products(odoo_df)
    if "Ubicación" in odoo_norm.columns:
        odoo_norm = odoo_norm.rename(columns={"Ubicación": "ALMACEN"})
    odoo_norm["ALMACEN"] = odoo_norm["ALMACEN"].astype(str).str.strip()

    stock_by_alm = (
        odoo_norm.groupby(["ALMACEN", "AmenityKey"], as_index=False)["Cantidad"]
        .sum()
        .rename(columns={"Cantidad": "Cantidad"})
    )

    urgent_only = mode.startswith("URGENTE")
    rep_all = summarize_replenishment(stock_by_alm, masters["thresholds"], objective="max", urgent_only=False)
    rep = summarize_replenishment(stock_by_alm, masters["thresholds"], objective="max", urgent_only=urgent_only)

    unclassified = odoo_norm[odoo_norm["AmenityKey"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        replenishment_df=rep,
        rep_all_df=rep_all,
        urgent_only=urgent_only,
        unclassified_products=unclassified,
        period_start=period_start,
        period_days=period_days,
    )

    # ---------------------------------------------------------
    # ✅ Lee Sheet UNA vez y prepara last_view
    # ---------------------------------------------------------
    sheet_df = None
    last_view = pd.DataFrame()
    try:
        sheet_df = read_sheet_df()
        if sheet_df is not None and not sheet_df.empty:
            last_view_raw = build_last_report_view(sheet_df)
            last_view = last_view_raw.rename(columns={
                "Apartamento": "APARTAMENTO",
                "Último informe": "MARCA_TEMPORAL",
                "LLAVES": "LLAVES",
                "OTRAS REPOSICIONES": "OTRAS_REPOSICIONES",
                "INCIDENCIAS/TAREAS A REALIZAR": "INCIDENCIAS",
            }).copy()
            last_view["APARTAMENTO"] = last_view["APARTAMENTO"].astype(str).str.strip().str.upper().map(_norm_apt_name)
    except Exception:
        sheet_df = None
        last_view = pd.DataFrame()

    # =========================
    # KPIs
    # =========================
    kpis = dash.get("kpis", {})
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Entradas (día foco)", kpis.get("entradas_dia", 0))
    c2.metric("Salidas (día foco)", kpis.get("salidas_dia", 0))
    c3.metric("Turnovers", kpis.get("turnovers_dia", 0))
    c4.metric("Ocupados", kpis.get("ocupados_dia", 0))
    c5.metric("Vacíos", kpis.get("vacios_dia", 0))

    # =========================
    # ✅ KPI: CHECK-INS PRESENCIALES HOY + DETALLE (clic)
    # =========================
    pres_today = _get_presenciales_today_df(dash.get("operativa", pd.DataFrame()))
    n_pres = int(len(pres_today)) if pres_today is not None else 0

    colP1, colP2 = st.columns([1, 3])
    with colP1:
        st.metric("Check-ins presenciales (hoy)", n_pres)
    with colP2:
        with st.expander("Ver cuáles (presenciales hoy)", expanded=False):
            if pres_today is None or pres_today.empty:
                st.info("No hay check-ins presenciales hoy.")
            else:
                show_cols = []
                for c in ["Día", "ZONA", "APARTAMENTO", "Cliente", "Estado", "Próxima Entrada", "Lista_reponer"]:
                    if c in pres_today.columns:
                        show_cols.append(c)
                if not show_cols:
                    show_cols = pres_today.columns.tolist()
                st.dataframe(pres_today[show_cols].reset_index(drop=True), use_container_width=True)

    # =========================================================
    # 🔎 BUSCADOR GLOBAL POR APARTAMENTO (sin selección por defecto)
    #    - No muestra nada hasta que pulses Enter en el input
    # =========================================================
    st.divider()
    st.subheader("🔎 Buscar apartamento · Resumen (Limpieza + Operativa + Reposición)")

    # Apartamentos: masters + operativa + sheet(last_view)
    apts_master = []
    try:
        if "apt_almacen" in masters and "APARTAMENTO" in masters["apt_almacen"].columns:
            apts_master = (
                masters["apt_almacen"]["APARTAMENTO"]
                .dropna()
                .astype(str)
                .str.strip()
                .str.upper()
                .map(_norm_apt_name)
                .unique()
                .tolist()
            )
    except Exception:
        apts_master = []

    apts_operativa = (
        dash["operativa"]["APARTAMENTO"].dropna().astype(str).str.strip().str.upper().map(_norm_apt_name).unique().tolist()
        if "operativa" in dash and isinstance(dash["operativa"], pd.DataFrame) and not dash["operativa"].empty
        else []
    )

    apts_sheet = (
        last_view["APARTAMENTO"].dropna().astype(str).str.strip().str.upper().map(_norm_apt_name).unique().tolist()
        if isinstance(last_view, pd.DataFrame) and not last_view.empty and "APARTAMENTO" in last_view.columns
        else []
    )

    apts_all = sorted(set([a for a in (apts_master + apts_operativa + apts_sheet) if a]))

    # Form para que SOLO ejecute al dar Enter o clicar botón
    with st.form("apt_search_form", clear_on_submit=False):
        q = st.text_input(
            "Buscar (escribe parte del nombre y pulsa Enter)",
            value="",
            placeholder="Ej: APOLO 29, BENICALAP, ALMIRANTE…",
        )
        submitted = st.form_submit_button("Buscar")

    # Si no se ha enviado, no mostramos nada (queda “en blanco”)
    if submitted:
        filt = q.strip().upper()
        if filt:
            apts_filtered = [a for a in apts_all if filt in a]
        else:
            apts_filtered = apts_all

        if not apts_filtered:
            st.info("No hay apartamentos que coincidan con esa búsqueda.")
        else:
            # Selectbox sin valor por defecto: ponemos "— Selecciona —" al principio
            options = ["— Selecciona apartamento —"] + apts_filtered
            apt_sel = st.selectbox("Selecciona apartamento", options=options, index=0, key="apt_sel_after_search")

            if apt_sel != "— Selecciona apartamento —":
                # --- 1) Limpieza (último informe) ---
                clean_row = None
                if isinstance(last_view, pd.DataFrame) and not last_view.empty:
                    sub = last_view[last_view["APARTAMENTO"] == apt_sel].copy()
                    if not sub.empty:
                        clean_row = sub.iloc[0].to_dict()

                # --- 2) Operativa del periodo ---
                op_df = dash["operativa"].copy()
                op_df["APARTAMENTO"] = op_df["APARTAMENTO"].astype(str).str.strip().str.upper().map(_norm_apt_name)
                op_sub = op_df[op_df["APARTAMENTO"] == apt_sel].copy()
                if "Día" in op_sub.columns:
                    op_sub = op_sub.sort_values("Día")

                # --- 3) Reposición del periodo (por ese apartamento) ---
                rep_rows = op_sub.copy()
                rep_cols = [c for c in ["Día", "ZONA", "Estado", "Lista_reponer", "Completar con"] if c in rep_rows.columns]
                rep_rows = rep_rows[rep_cols].copy() if rep_cols else pd.DataFrame()

                items_rows = []
                if not rep_rows.empty:
                    cols_src = []
                    if "Lista_reponer" in rep_rows.columns:
                        cols_src.append("Lista_reponer")
                    if "Completar con" in rep_rows.columns:
                        cols_src.append("Completar con")
                    for _, r in rep_rows.iterrows():
                        for col in cols_src:
                            txt = r.get(col, "")
                            if str(txt).strip() == "":
                                continue
                            for prod, qty in parse_lista_reponer(txt):
                                items_rows.append({
                                    "Día": r.get("Día"),
                                    "Producto": prod,
                                    "Cantidad": int(qty),
                                    "Fuente": col,
                                })

                items_df_apt = pd.DataFrame(items_rows)
                if not items_df_apt.empty:
                    totals_apt = (
                        items_df_apt.groupby("Producto", as_index=False)["Cantidad"]
                        .sum()
                        .sort_values(["Cantidad", "Producto"], ascending=[False, True])
                        .reset_index(drop=True)
                    )
                else:
                    totals_apt = pd.DataFrame(columns=["Producto", "Cantidad"])

                tab1, tab2, tab3 = st.tabs(["Resumen", "Operativa", "Reposición"])

                with tab1:
                    cA, cB = st.columns([1, 1])

                    with cA:
                        st.markdown("**🧽 Último informe de limpieza (Google Sheet)**")
                        if clean_row is None:
                            st.info("Sin registro de limpieza para este apartamento (o no se pudo leer la Sheet).")
                        else:
                            ts = clean_row.get("MARCA_TEMPORAL", "")
                            if isinstance(ts, pd.Timestamp):
                                ts_txt = ts.strftime("%d/%m/%Y %H:%M")
                            else:
                                ts_txt = str(ts)

                            st.write(f"**Marca temporal:** {ts_txt}")
                            st.write(f"**Llaves:** {clean_row.get('LLAVES','')}")
                            st.write(f"**Otras reposiciones:** {clean_row.get('OTRAS_REPOSICIONES','')}")
                            st.write(f"**Incidencias:** {clean_row.get('INCIDENCIAS','')}")

                    with cB:
                        st.markdown("**📅 Operativa (periodo seleccionado)**")
                        if op_sub.empty:
                            st.info("Este apartamento no aparece en la operativa del periodo.")
                        else:
                            show = op_sub.copy()
                            cols_show = [c for c in ["Día", "Estado", "ZONA", "Cliente", "Lista_reponer", "Completar con", "Próxima Entrada"] if c in show.columns]
                            if not cols_show:
                                cols_show = show.columns.tolist()[:10]
                            st.dataframe(show[cols_show].reset_index(drop=True), use_container_width=True, height=220)

                with tab2:
                    st.markdown("**Operativa completa (solo este apartamento)**")
                    if op_sub.empty:
                        st.info("Sin filas de operativa en el periodo.")
                    else:
                        st.dataframe(op_sub.reset_index(drop=True), use_container_width=True)

                with tab3:
                    st.markdown("**Totales de reposición (solo este apartamento, periodo)**")
                    if totals_apt.empty:
                        st.info("No hay reposición detectada en Lista_reponer / Completar con para este apartamento.")
                    else:
                        st.dataframe(totals_apt, use_container_width=True, height=260)

                    st.markdown("**Detalle por día/fuente**")
                    if items_df_apt.empty:
                        st.info("Sin detalle de reposición.")
                    else:
                        st.dataframe(items_df_apt.sort_values(["Día", "Producto"]).reset_index(drop=True), use_container_width=True)

    # ==============
    # BLOQUE 8: Sheet (día foco)
    # ==============
    st.divider()
    st.subheader("🧾 Incidencias / Faltantes / Café (Google Sheet) · Día foco")

    foco_date = pd.Timestamp(dash["period_start"])
    ops_today = pd.DataFrame(columns=["APARTAMENTO", "Incidencias hoy", "Faltantes por entrada", "Reposiciones café"])

    try:
        if sheet_df is None or sheet_df.empty:
            st.info("Google Sheet: sin datos (o no se pudo leer).")
        else:
            ops_today = _extract_ops_from_sheet(sheet_df, foco_date)
            with st.expander("Ver detalle (hoy)", expanded=False):
                st.dataframe(ops_today, use_container_width=True)
    except Exception as e:
        st.warning("No pude procesar el Google Sheet (día foco).")
        st.exception(e)

    # =========================================================
    # ✅ Tabla del último informe (opcional)
    # =========================================================
    if sheet_df is not None and not sheet_df.empty and isinstance(last_view, pd.DataFrame) and not last_view.empty:
        st.divider()
        st.subheader("🧩 Último informe de limpieza por apartamento (según Marca temporal)")
        only_alerts_last = st.toggle(
            "Mostrar solo apartamentos con algo que revisar",
            value=True,
            key="only_alerts_last",
        )
        view_to_show = last_view.copy()
        if only_alerts_last:
            view_to_show = view_to_show[
                view_to_show["flag_llaves"] | view_to_show["flag_otras_repos"] | view_to_show["flag_incidencias"]
            ].copy()

        show_cols = ["APARTAMENTO", "MARCA_TEMPORAL", "LLAVES", "OTRAS_REPOSICIONES", "INCIDENCIAS"]
        show_df_last = view_to_show[show_cols].copy()
        if pd.api.types.is_datetime64_any_dtype(show_df_last["MARCA_TEMPORAL"]):
            show_df_last["MARCA_TEMPORAL"] = show_df_last["MARCA_TEMPORAL"].dt.strftime("%d/%m/%Y %H:%M")

        st.dataframe(show_df_last, use_container_width=True)

    st.download_button(
        "⬇️ Descargar Excel (Operativa)",
        data=dash["excel_all"],
        file_name=dash["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # =========================
    # 1) PARTE OPERATIVO
    # =========================
    st.divider()
    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

    operativa = dash["operativa"].copy()

    # ✅ MERGE: Operativa + ÚLTIMO INFORME (Sheet)
    try:
        if isinstance(last_view, pd.DataFrame) and not last_view.empty:
            operativa["APARTAMENTO"] = operativa["APARTAMENTO"].astype(str).str.strip().str.upper().map(_norm_apt_name)
            operativa = operativa.merge(
                last_view[["APARTAMENTO", "MARCA_TEMPORAL", "LLAVES", "OTRAS_REPOSICIONES", "INCIDENCIAS"]],
                on="APARTAMENTO",
                how="left",
            )
    except Exception as e:
        st.warning("No pude enlazar Operativa con el último informe del Google Sheet.")
        st.exception(e)

    if zonas_sel:
        operativa = operativa[operativa["ZONA"].isin(zonas_sel)].copy()
    if estados_sel:
        operativa = operativa[operativa["Estado"].isin(estados_sel)].copy()

    operativa = operativa.sort_values(["Día", "ZONA", "__prio", "APARTAMENTO"])

    for dia, ddf in operativa.groupby("Día", dropna=False):
        st.markdown(f"### Día {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
        if ddf.empty:
            st.info("Sin datos.")
            continue

        for zona, zdf in ddf.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
            st.markdown(f"#### {zona_label}")
            show_df = zdf.drop(columns=["ZONA", "__prio"], errors="ignore").copy()
            st.dataframe(
                _style_operativa(show_df),
                use_container_width=True,
                height=min(520, 40 + 35 * len(show_df)),
            )

    # =========================
    # 2) SUGERENCIA DE REPOSICIÓN
    # =========================
    st.divider()
    st.subheader("Sugerencia de Reposición")

    if urgent_only:
        st.caption("Modo URGENTE: Totales + dónde dejar, incluyendo Lista_reponer (urgente) y Completar con.")
        items_df, totals_df = build_sugerencia_df(dash["operativa"], zonas_sel, include_completar=True)
    else:
        st.caption("Resumen del periodo: ENTRADA / ENTRADA+SALIDA / VACIO con reposición. Totales + dónde dejar.")
        items_df, totals_df = build_sugerencia_df(dash["operativa"], zonas_sel, include_completar=False)

    if items_df.empty:
        st.info("No hay reposición sugerida para el periodo (con esos criterios) o faltan listas.")
    else:
        colA, colB = st.columns([1, 2])
        with colA:
            st.markdown("**Totales (preparar carrito)**")
            st.dataframe(totals_df, use_container_width=True, height=min(520, 40 + 35 * len(totals_df)))
        with colB:
            st.markdown("**Dónde dejar cada producto** (por ZONA y APARTAMENTO)")
            st.dataframe(items_df, use_container_width=True, height=min(520, 40 + 28 * min(len(items_df), 25)))

    # =========================
    # 3) RUTAS GOOGLE MAPS
    # =========================
    st.divider()
    st.subheader("📍 Ruta Google Maps · Reposición HOY + MAÑANA (por ZONA)")
    st.caption("Criterio: con reposición y Estado == VACIO o ENTRADA o ENTRADA+SALIDA ese día. Botones directos a Maps.")

    tz = ZoneInfo("Europe/Madrid")
    today = pd.Timestamp.now(tz=tz).normalize().date()
    tomorrow = (pd.Timestamp(today) + pd.Timedelta(days=1)).date()

    visitable_states = {"VACIO", "ENTRADA", "ENTRADA+SALIDA"}

    route_df = dash["operativa"].copy()
    route_df = route_df[route_df["Día"].isin([today, tomorrow])].copy()
    route_df = route_df[route_df["Estado"].isin(visitable_states)].copy()
    route_df = route_df[route_df["Lista_reponer"].astype(str).str.strip().ne("")].copy()

    if zonas_sel:
        route_df = route_df[route_df["ZONA"].isin(zonas_sel)].copy()

    route_df = route_df.merge(ap_map[["APARTAMENTO", "LAT", "LNG"]], on="APARTAMENTO", how="left")
    route_df["COORD"] = route_df.apply(lambda r: _coord_str(r.get("LAT"), r.get("LNG")), axis=1)
    route_df = route_df[route_df["COORD"].notna()].copy()

    if route_df.empty:
        st.info("No hay apartamentos visitables con reposición para HOY/MAÑANA (o faltan coordenadas).")
    else:
        MAX_STOPS = 20
        for dia, ddf in route_df.groupby("Día", dropna=False):
            st.markdown(f"### {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
            for zona, zdf in ddf.groupby("ZONA", dropna=False):
                zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
                coords = zdf["COORD"].tolist()
                if not coords:
                    st.info(f"{zona_label}: sin coordenadas suficientes.")
                    continue

                for idx, chunk in enumerate(chunk_list(coords, MAX_STOPS), start=1):
                    url = build_gmaps_directions_url(chunk, travelmode=travelmode, return_to_base=return_to_base)
                    if url:
                        st.link_button(f"Abrir ruta · {zona_label} (tramo {idx})", url)

    with st.expander("🧪 Debug reposición (por almacén)", expanded=False):
        st.caption("Comprueba Min/Max/Stock y el cálculo final.")
        st.dataframe(rep.sort_values(["ALMACEN", "Amenity"], na_position="last").reset_index(drop=True), use_container_width=True)
        if not unclassified.empty:
            st.warning("Hay productos sin clasificar (no entran en reposición).")
            st.dataframe(unclassified.reset_index(drop=True), use_container_width=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
