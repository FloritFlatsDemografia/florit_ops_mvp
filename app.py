import streamlit as st
import pandas as pd
from zoneinfo import ZoneInfo
from urllib.parse import quote
import re
import unicodedata

ORIGIN_LAT = 39.45702028460933
ORIGIN_LNG = -0.38498336081567713


# =========================
# Apartamento key (matching robusto)
# =========================
def _apt_key(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # quita tildes
    s = re.sub(r"\s+", " ", s)
    # Quita ceros iniciales en números sueltos: "APOLO 029" -> "APOLO 29"
    s = re.sub(r"\b0+(\d)", r"\1", s)
    return s.upper().strip()


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
# ✅ WRAP de texto largo para que NO se "corte" visualmente
# (en st.dataframe, si metes saltos de línea, se ve todo)
# =========================
_LONG_COLS = ["Lista_reponer", "Completar con"]


def _wrap_commas_to_newlines(x: object) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    # Inserta saltos de línea después de comas para que Streamlit envuelva el texto.
    s = re.sub(r"\s*,\s*", ",\n", s)
    return s


def _apply_wrap_long_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in _LONG_COLS:
        if c in out.columns:
            out[c] = out[c].apply(_wrap_commas_to_newlines)
    return out


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


def _df_column_config_for_long_text(df: pd.DataFrame) -> dict:
    cfg = {}
    for c in _LONG_COLS:
        if c in df.columns:
            cfg[c] = st.column_config.TextColumn(
                c,
                help="Texto completo (con saltos de línea).",
                width="large",
            )
    return cfg


def _render_operativa_table(
    df: pd.DataFrame,
    key: str,
    height: int | str | None = "content",
    styled: bool = True,
):
    """
    Render estándar: envuelve texto largo y lo muestra sin 'cortes' visuales.
    IMPORTANTE: Streamlit NO admite height=None. Aquí:
      - Si height es None -> se omite el argumento height.
      - Si no, se pasa (int, 'content', 'stretch').
    """
    if df is None or df.empty:
        st.info("Sin resultados.")
        return

    view = _apply_wrap_long_cols(df)
    colcfg = _df_column_config_for_long_text(view)

    kwargs = dict(use_container_width=True, column_config=colcfg)
    if height is not None:
        kwargs["height"] = height

    if styled:
        st.dataframe(_style_operativa(view), **kwargs)
    else:
        st.dataframe(view, **kwargs)


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
# Google Sheet helpers
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


def _kpi_table(df: pd.DataFrame, title: str):
    st.markdown(f"#### {title}")
    if df is None or df.empty:
        st.info("Sin resultados.")
        return
    cols_show = [
        c
        for c in [
            "Día",
            "ZONA",
            "APARTAMENTO",
            "Cliente",
            "Estado",
            "Próxima Entrada",
            "Lista_reponer",
            "Completar con",
        ]
        if c in df.columns
    ]
    view = df[cols_show].copy()
    _render_operativa_table(view, key=f"kpi_{_apt_key(title)}", height="content", styled=False)


def main():
    from src.loaders import load_masters_repo
    from src.parsers import parse_avantio_entradas, parse_odoo_stock
    from src.normalize import normalize_products, summarize_replenishment
    from src.dashboard import build_dashboard_frames
    from src.gsheets import read_sheet_df

    try:
        from src.cleaning_last_report import build_last_report_view
    except Exception:
        from src.parsers.cleaning_last_report import build_last_report_view

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

    # Parse
    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)
    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant.")
        st.stop()

    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()
    avantio_df["APARTAMENTO_KEY"] = avantio_df["APARTAMENTO"].map(_apt_key)

    # Maestros
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

    # Stock normalize
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

    # Dashboard
    if "kpi_open" not in st.session_state:
        st.session_state["kpi_open"] = ""

    tz = ZoneInfo("Europe/Madrid")
    today = pd.Timestamp.now(tz=tz).normalize().date()
    foco_day = pd.Timestamp(dash.get("period_start")).normalize().date()

    oper_all = dash["operativa"].copy()
    oper_all["APARTAMENTO_KEY"] = oper_all["APARTAMENTO"].map(_apt_key)
    oper_foco = oper_all[oper_all["Día"] == foco_day].copy()

    presencial_set = {"APOLO 029", "APOLO 180", "APOLO 197", "SERRANOS"}
    presencial_keys = {_apt_key(x) for x in presencial_set}

    pres_today = oper_all[
        (oper_all["Día"] == today)
        & (oper_all["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA"]))
        & (oper_all["APARTAMENTO_KEY"].isin(presencial_keys))
    ].copy()

    kpis = dash.get("kpis", {})
    st.divider()
    st.subheader("📊 Dashboard (día foco)")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Entradas (día foco)", kpis.get("entradas_dia", 0))
        if st.button("Ver entradas", key="kpi_btn_entradas"):
            st.session_state["kpi_open"] = "entradas"
    with c2:
        st.metric("Salidas (día foco)", kpis.get("salidas_dia", 0))
        if st.button("Ver salidas", key="kpi_btn_salidas"):
            st.session_state["kpi_open"] = "salidas"
    with c3:
        st.metric("Turnovers", kpis.get("turnovers_dia", 0))
        if st.button("Ver turnovers", key="kpi_btn_turnovers"):
            st.session_state["kpi_open"] = "turnovers"
    with c4:
        st.metric("Ocupados", kpis.get("ocupados_dia", 0))
        if st.button("Ver ocupados", key="kpi_btn_ocupados"):
            st.session_state["kpi_open"] = "ocupados"
    with c5:
        st.metric("Vacíos", kpis.get("vacios_dia", 0))
        if st.button("Ver vacíos", key="kpi_btn_vacios"):
            st.session_state["kpi_open"] = "vacios"
    with c6:
        st.metric("Check-ins presenciales (HOY)", int(len(pres_today)))
        if st.button("Ver presenciales", key="kpi_btn_presenciales"):
            st.session_state["kpi_open"] = "presenciales"

    kpi_open = st.session_state.get("kpi_open", "")
    if kpi_open:
        st.divider()
        st.subheader("📌 Detalle KPI")

        if kpi_open == "entradas":
            df = oper_foco[oper_foco["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA"])].copy()
            _kpi_table(df, f"Entradas · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")
        elif kpi_open == "salidas":
            df = oper_foco[oper_foco["Estado"].isin(["SALIDA", "ENTRADA+SALIDA"])].copy()
            _kpi_table(df, f"Salidas · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")
        elif kpi_open == "turnovers":
            df = oper_foco[oper_foco["Estado"].isin(["ENTRADA+SALIDA"])].copy()
            _kpi_table(df, f"Turnovers · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")
        elif kpi_open == "ocupados":
            df = oper_foco[oper_foco["Estado"].isin(["OCUPADO"])].copy()
            _kpi_table(df, f"Ocupados · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")
        elif kpi_open == "vacios":
            df = oper_foco[oper_foco["Estado"].isin(["VACIO"])].copy()
            _kpi_table(df, f"Vacíos · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")
        elif kpi_open == "presenciales":
            _kpi_table(pres_today, f"Check-ins presenciales · HOY {pd.to_datetime(today).strftime('%d/%m/%Y')}")

    # Descarga
    st.download_button(
        "⬇️ Descargar Excel (Operativa)",
        data=dash["excel_all"],
        file_name=dash["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Parte completo
    st.divider()
    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

    operativa = dash["operativa"].copy()
    operativa["APARTAMENTO_KEY"] = operativa["APARTAMENTO"].map(_apt_key)

    if zonas_sel:
        operativa = operativa[operativa["ZONA"].isin(zonas_sel)].copy()
    if estados_sel:
        operativa = operativa[operativa["Estado"].isin(estados_sel)].copy()

    operativa = operativa.sort_values(["Día", "ZONA", "__prio", "APARTAMENTO"])

    for dia, ddf in operativa.groupby("Día", dropna=False):
        st.markdown(f"### Día {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
        for zona, zdf in ddf.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
            st.markdown(f"#### {zona_label}")
            show_df = zdf.drop(columns=["ZONA", "__prio", "APARTAMENTO_KEY"], errors="ignore").copy()
            _render_operativa_table(
                show_df,
                key=f"oper_{pd.to_datetime(dia).strftime('%Y%m%d')}_{_apt_key(str(zona_label))}",
                height=min(520, 40 + 35 * len(show_df)),
                styled=True,
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
