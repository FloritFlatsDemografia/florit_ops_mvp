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
    s = re.sub(r"\b0+(\d)", r"\1", s)  # "APOLO 029" -> "APOLO 29"
    return s.upper().strip()


# =========================
# Excel-letter column helper
# =========================
def _col_by_excel_letter(df: pd.DataFrame, letter: str) -> str:
    """
    Devuelve el nombre de columna por letra Excel (A=0, B=1, ..., Z=25, AA=26...)
    OJO: esto SOLO es fiable si el DF conserva el orden exacto del Excel original.
    """
    letter = letter.upper().strip()
    idx = 0
    for ch in letter:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    idx -= 1
    if idx < 0 or idx >= len(df.columns):
        raise KeyError(f"No existe la columna {letter} en el DF (tamaño {len(df.columns)}).")
    return df.columns[idx]


# =========================
# Hora check-in (default 16:00)
# =========================
def _parse_time_to_hhmm(x) -> str:
    if x is None:
        return "16:00"
    try:
        if isinstance(x, float) and pd.isna(x):
            return "16:00"
    except Exception:
        pass

    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return "16:00"

    s = s.lower().replace("h", "").replace(".", ":").strip()

    s_num = s.replace(",", ".")
    try:
        if ":" not in s_num and re.fullmatch(r"[0-9]+(\.[0-9]+)?", s_num):
            hh = int(float(s_num))
            hh = max(0, min(23, hh))
            return f"{hh:02d}:00"
    except Exception:
        pass

    try:
        parts = s.split(":")
        if len(parts) >= 2:
            hh = int(parts[0])
            mm = int(parts[1])
            hh = max(0, min(23, hh))
            mm = max(0, min(59, mm))
            return f"{hh:02d}:{mm:02d}"
        if len(parts) == 1 and parts[0].isdigit():
            hh = int(parts[0])
            hh = max(0, min(23, hh))
            return f"{hh:02d}:00"
    except Exception:
        pass

    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%H:%M")
    except Exception:
        pass

    return "16:00"


# =========================
# Teléfono helpers
# =========================
def _clean_phone(x) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, float) and pd.isna(x):
            return ""
    except Exception:
        pass

    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""

    s = s.replace("\u00A0", " ").strip()
    has_plus = s.startswith("+")
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return ""
    return ("+" if has_plus else "") + digits


# =========================
# Acceso helpers
# =========================
def _norm_acceso(x) -> str:
    """
    Normaliza:
    - Presencial
    - Hoomvip
    - Hoomvip + Candado
    - Candado
    Si viene vacío -> Presencial
    """
    if x is None:
        return "Presencial"
    try:
        if isinstance(x, float) and pd.isna(x):
            return "Presencial"
    except Exception:
        pass

    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return "Presencial"

    s_low = s.lower()
    if "hoomvip" in s_low and "candado" in s_low:
        return "Hoomvip + Candado"
    if "hoomvip" in s_low:
        return "Hoomvip"
    if "candado" in s_low:
        return "Candado"
    if "presencial" in s_low:
        return "Presencial"
    return s[:1].upper() + s[1:]


def _detect_acceso_col(df: pd.DataFrame) -> str | None:
    """
    Detecta la columna 'Acceso' del maestro de forma robusta:
    1) Por nombre (exacto / contiene 'acceso')
    2) Por contenido (la que más veces contiene presencial/hoomvip/candado)
    """
    if df is None or df.empty:
        return None

    # 1) Por nombre
    for c in df.columns:
        if str(c).strip().lower() == "acceso":
            return c
    for c in df.columns:
        if "acceso" in str(c).strip().lower():
            return c

    # 2) Por contenido
    pat = r"(presencial|hoomvip|candado)"
    best = None
    best_score = 0
    for c in df.columns:
        try:
            s = df[c].astype(str).str.lower()
            score = int(s.str.contains(pat, na=False).sum())
            if score > best_score:
                best_score = score
                best = c
        except Exception:
            continue

    if best_score > 0:
        return best
    return None


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
# Render helper (NO HTML) + no cortar texto
# =========================
def _render_operativa_table(df: pd.DataFrame, key: str, styled: bool = True):
    if df is None or df.empty:
        st.info("Sin resultados.")
        return

    view = df.copy()

    colcfg = {}
    for c in ["Lista_reponer", "Completar con", "Producto", "Cliente"]:
        if c in view.columns:
            colcfg[c] = st.column_config.TextColumn(c, width="large", max_chars=10000)

    if "APARTAMENTO" in view.columns:
        colcfg["APARTAMENTO"] = st.column_config.TextColumn("APARTAMENTO", width="medium", max_chars=5000)

    if "Acceso" in view.columns:
        colcfg["Acceso"] = st.column_config.TextColumn("Acceso", width="medium", max_chars=200)

    if "Teléfono" in view.columns:
        colcfg["Teléfono"] = st.column_config.TextColumn("Teléfono", width="medium", max_chars=200)

    if styled:
        st.dataframe(_style_operativa(view), use_container_width=True, height="content", column_config=colcfg)
    else:
        st.dataframe(view, use_container_width=True, height="content", column_config=colcfg)


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
# KPI table
# =========================
def _kpi_table(df: pd.DataFrame, title: str):
    if df is None or df.empty:
        st.info("Sin resultados.")
        return

    cols_show = [
        c
        for c in [
            "Día",
            "ZONA",
            "APARTAMENTO",
            "Acceso",
            "Cliente",
            "Teléfono",
            "Nº Adultos",
            "Nº Niños",
            "Hora Check-in",
            "Estado",
            "Lista_reponer",
            "Completar con",
        ]
        if c in df.columns
    ]

    st.markdown(f"#### {title}")
    view = df[cols_show].reset_index(drop=True)
    _render_operativa_table(view, key=f"kpi_{_apt_key(title)}", styled=False)


# =========================
# Enriquecimiento: adultos/niños/hora check-in/teléfono desde Avantio (Entradas)
# =========================
def _detect_checkin_datetime_col(avantio_df: pd.DataFrame) -> str | None:
    for c in avantio_df.columns:
        cl = str(c).lower()
        if "fecha" in cl and "entrada" in cl:
            return c
        if "check" in cl and "in" in cl:
            return c
    try:
        return _col_by_excel_letter(avantio_df, "D")
    except Exception:
        return None


def enrich_operativa_with_guest_fields(operativa_df: pd.DataFrame, avantio_df: pd.DataFrame) -> pd.DataFrame:
    out = operativa_df.copy()

    if "Nº Adultos" not in out.columns:
        out["Nº Adultos"] = 0
    if "Nº Niños" not in out.columns:
        out["Nº Niños"] = 0
    if "Hora Check-in" not in out.columns:
        out["Hora Check-in"] = "16:00"
    if "Teléfono" not in out.columns:
        out["Teléfono"] = ""

    if out is None or out.empty:
        return out
    if avantio_df is None or avantio_df.empty:
        return out

    av = avantio_df.copy()

    if "APARTAMENTO" not in av.columns and "Alojamiento" in av.columns:
        av["APARTAMENTO"] = av["Alojamiento"].astype(str).str.strip()

    if "APARTAMENTO" not in av.columns:
        return out

    av["APARTAMENTO"] = av["APARTAMENTO"].astype(str).str.strip()
    av["APARTAMENTO_KEY"] = av["APARTAMENTO"].map(_apt_key)

    try:
        col_ad = _col_by_excel_letter(av, "H")
        col_ch = _col_by_excel_letter(av, "I")
        col_ci = _col_by_excel_letter(av, "Z")
        col_tel = _col_by_excel_letter(av, "N")
    except Exception:
        return out

    dtcol = _detect_checkin_datetime_col(av)
    if dtcol is None:
        return out

    av["_CHECKIN_DT"] = pd.to_datetime(av[dtcol], errors="coerce")
    av["_CHECKIN_DATE"] = av["_CHECKIN_DT"].dt.date

    av["AV_ADULTOS"] = pd.to_numeric(av[col_ad], errors="coerce").fillna(0).astype(int)
    av["AV_NINOS"] = pd.to_numeric(av[col_ch], errors="coerce").fillna(0).astype(int)
    av["AV_CHECKIN"] = av[col_ci].apply(_parse_time_to_hhmm)
    av["AV_TEL"] = av[col_tel].apply(_clean_phone)

    av_small = (
        av.dropna(subset=["APARTAMENTO_KEY", "_CHECKIN_DATE"])
        .sort_values(["APARTAMENTO_KEY", "_CHECKIN_DATE"])
        .groupby(["APARTAMENTO_KEY", "_CHECKIN_DATE"], as_index=False)
        .agg({"AV_ADULTOS": "first", "AV_NINOS": "first", "AV_CHECKIN": "first", "AV_TEL": "first"})
        .rename(columns={"_CHECKIN_DATE": "Día"})
    )

    if "APARTAMENTO_KEY" not in out.columns:
        out["APARTAMENTO_KEY"] = out["APARTAMENTO"].map(_apt_key)

    out["Día"] = pd.to_datetime(out["Día"], errors="coerce").dt.date
    out = out.merge(av_small, on=["APARTAMENTO_KEY", "Día"], how="left")

    out["Nº Adultos"] = pd.to_numeric(out["AV_ADULTOS"], errors="coerce").fillna(out["Nº Adultos"]).fillna(0).astype(int)
    out["Nº Niños"] = pd.to_numeric(out["AV_NINOS"], errors="coerce").fillna(out["Nº Niños"]).fillna(0).astype(int)
    out["Hora Check-in"] = out["AV_CHECKIN"].fillna(out["Hora Check-in"]).apply(_parse_time_to_hhmm)
    out["Teléfono"] = out["AV_TEL"].fillna(out["Teléfono"]).fillna("")

    out = out.drop(columns=["AV_ADULTOS", "AV_NINOS", "AV_CHECKIN", "AV_TEL"], errors="ignore")
    return out


# =========================
# Localización helper (para sacar lat/lng)
# =========================
def _find_loc_col(cols) -> str | None:
    for c in cols:
        cl = str(c).lower().strip()
        cl = (
            cl.replace("ó", "o")
            .replace("í", "i")
            .replace("á", "a")
            .replace("é", "e")
            .replace("ú", "u")
        )
        if "local" in cl:
            return c
    return None


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

    # =========================
    # Parse ficheros
    # =========================
    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)
    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant.")
        st.stop()

    # Normaliza APARTAMENTO (Avantio)
    if "Alojamiento" in avantio_df.columns:
        avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()
    elif "APARTAMENTO" in avantio_df.columns:
        avantio_df["APARTAMENTO"] = avantio_df["APARTAMENTO"].astype(str).str.strip()
    else:
        st.error("Avantio (Entradas): no encuentro columna 'Alojamiento' ni 'APARTAMENTO'.")
        st.stop()

    avantio_df["APARTAMENTO_KEY"] = avantio_df["APARTAMENTO"].map(_apt_key)

    # Maestros
    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")

    # =========================
    # Maestro apt_almacen (Apartamentos e Inventarios)
    # =========================
    ap_map = masters["apt_almacen"].copy()

    need = {"APARTAMENTO", "ALMACEN"}
    if not need.issubset(set(ap_map.columns)):
        st.error(f"Maestro apt_almacen: faltan columnas {need}. Columnas: {list(ap_map.columns)}")
        st.stop()

    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()
    ap_map["APARTAMENTO_KEY"] = ap_map["APARTAMENTO"].map(_apt_key)

    # ✅ Acceso: NO usamos "columna D". Detectamos columna real por nombre o contenido.
    acc_col = _detect_acceso_col(ap_map)
    if acc_col is None:
        ap_map["Acceso"] = "Presencial"
    else:
        ap_map["Acceso"] = ap_map[acc_col].apply(_norm_acceso)

    # Coordenadas desde "Localización"
    for c in ["LAT", "LNG"]:
        if c not in ap_map.columns:
            ap_map[c] = pd.NA

    loc_col = _find_loc_col(ap_map.columns)

    def _split_loc(x):
        s = str(x).strip()
        if "," in s:
            a, b = s.split(",", 1)
            return a.strip(), b.strip()
        return None, None

    if loc_col is not None:
        miss = ap_map["LAT"].isna() | ap_map["LNG"].isna()
        if miss.any():
            loc_pairs = ap_map.loc[miss, loc_col].apply(_split_loc)
            ap_map.loc[miss, "LAT"] = [p[0] for p in loc_pairs]
            ap_map.loc[miss, "LNG"] = [p[1] for p in loc_pairs]

    ap_map = (
        ap_map[["APARTAMENTO", "APARTAMENTO_KEY", "ALMACEN", "LAT", "LNG", "Acceso"]]
        .dropna(subset=["APARTAMENTO_KEY", "ALMACEN"])
        .drop_duplicates(subset=["APARTAMENTO_KEY"], keep="first")
    )

    # Merge coords/almacén a Avantio (rutas)
    avantio_df = avantio_df.merge(ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]], on="APARTAMENTO", how="left")

    # =========================
    # Stock normalize
    # =========================
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

    # =========================
    # DASHBOARD KPIs + detalle
    # =========================
    if "kpi_open" not in st.session_state:
        st.session_state["kpi_open"] = ""

    tz = ZoneInfo("Europe/Madrid")
    today_real = pd.Timestamp.now(tz=tz).normalize().date()
    foco_day = pd.Timestamp(dash.get("period_start")).normalize().date()

    oper_all = dash["operativa"].copy()
    oper_all["APARTAMENTO_KEY"] = oper_all["APARTAMENTO"].map(_apt_key)

    # ✅ Acceso fijo del maestro (merge por KEY)
    oper_all = oper_all.merge(ap_map[["APARTAMENTO_KEY", "Acceso"]], on="APARTAMENTO_KEY", how="left")
    oper_all["Acceso"] = oper_all["Acceso"].apply(_norm_acceso)

    # Enriquecemos (adultos/niños/checkin/teléfono)
    oper_all = enrich_operativa_with_guest_fields(oper_all, avantio_df)

    oper_foco = oper_all[oper_all["Día"] == foco_day].copy()

    # Presenciales (tu lógica original)
    presencial_set = {"APOLO 029", "APOLO 180", "APOLO 197", "SERRANOS"}
    presencial_keys = {_apt_key(x) for x in presencial_set}
    pres_df = oper_all[
        (oper_all["Día"] == foco_day)
        & (oper_all["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA"]))
        & (oper_all["APARTAMENTO_KEY"].isin(presencial_keys))
    ].copy()
    pres_label = "HOY" if foco_day == today_real else pd.to_datetime(foco_day).strftime("%d/%m/%Y")

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
        st.metric(f"Check-ins presenciales ({pres_label})", int(len(pres_df)))
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
            _kpi_table(pres_df, f"Check-ins presenciales · {pres_label}")

        st.caption("Para cerrar, pulsa otro KPI o recarga la página.")

    # =========================
    # Descarga Excel
    # =========================
    st.download_button(
        "⬇️ Descargar Excel (Operativa)",
        data=dash["excel_all"],
        file_name=dash["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # =========================
    # PARTE OPERATIVO COMPLETO
    # =========================
    st.divider()
    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

    operativa = oper_all.copy()

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
            show_df = zdf.drop(columns=["ZONA", "__prio", "APARTAMENTO_KEY"], errors="ignore").copy()
            _render_operativa_table(
                show_df,
                key=f"oper_{pd.to_datetime(dia).strftime('%Y%m%d')}_{_apt_key(str(zona_label))}",
                styled=True,
            )

    # =========================
    # SUGERENCIA DE REPOSICIÓN
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
            st.dataframe(totals_df, use_container_width=True, height="content")
        with colB:
            st.markdown("**Dónde dejar cada producto** (por ZONA y APARTAMENTO)")
            _render_operativa_table(items_df, key="sugerencia_items", styled=False)

    # =========================
    # RUTAS GOOGLE MAPS
    # =========================
    st.divider()
    st.subheader("📍 Ruta Google Maps · Reposición HOY + MAÑANA (por ZONA)")
    st.caption("Criterio: con reposición y Estado == VACIO o ENTRADA o ENTRADA+SALIDA ese día. Botones directos a Maps.")

    tomorrow = (pd.Timestamp(today_real) + pd.Timedelta(days=1)).date()
    visitable_states = {"VACIO", "ENTRADA", "ENTRADA+SALIDA"}

    route_df = dash["operativa"].copy()
    route_df = route_df[route_df["Día"].isin([today_real, tomorrow])].copy()
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
        st.dataframe(
            rep.sort_values(["ALMACEN", "Amenity"], na_position="last").reset_index(drop=True),
            use_container_width=True,
            height="content",
        )
        if not unclassified.empty:
            st.warning("Hay productos sin clasificar (no entran en reposición).")
            st.dataframe(unclassified.reset_index(drop=True), use_container_width=True, height="content")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
