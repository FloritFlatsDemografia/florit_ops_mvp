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


def _safe_height(n_rows: int, row_px: int = 34, base_px: int = 80, max_px: int = 680) -> int:
    # Streamlit NO acepta None; siempre int > 0
    h = base_px + row_px * max(1, int(n_rows))
    return int(min(max_px, max(220, h)))


def _render_operativa_table(df: pd.DataFrame, key: str, styled: bool = True):
    if df is None or df.empty:
        st.info("Sin resultados.")
        return

    view = df.copy()

    # Column_config para favorecer lectura; no "soluciona" truncado visual,
    # pero ayuda + dejamos "visor de texto completo" debajo.
    colcfg = {}
    for c in ["Lista_reponer", "Completar con"]:
        if c in view.columns:
            colcfg[c] = st.column_config.TextColumn(
                c,
                width="large",
            )

    height = _safe_height(len(view))

    if styled:
        st.dataframe(_style_operativa(view), use_container_width=True, height=height, column_config=colcfg)
    else:
        st.dataframe(view, use_container_width=True, height=height, column_config=colcfg)


# =========================
# Reposición parsing + normalización
# =========================
_ITEM_RX = re.compile(r"^\s*(.*?)\s*x\s*([0-9]+)\s*$", re.IGNORECASE)

DISPLAY_MAP = {
    # Lo que tú quieres ver “bonito”
    _apt_key("Capsula Tassimo"): "Tassimo",
    _apt_key("Café en cápsula Colombia"): "Nespresso",
    _apt_key("Champu Rituals"): "Champu",
    _apt_key("Café Natural Molido"): "Café Molido",
    _apt_key("Azúcar blanco en sobres Hacendado - Caja / 50 sobres"): "Caja Azucar",
    # el resto se deja tal cual (p.ej. Detergente, Gel de manos...)
}

COFFEE_BY_TIPO = {
    "Tassimo": _apt_key("Capsula Tassimo"),
    "Nespresso": _apt_key("Café en cápsula Colombia"),
    "Molido": _apt_key("Café Natural Molido"),
}

def _display_name(amenity: str) -> str:
    k = _apt_key(amenity)
    return DISPLAY_MAP.get(k, amenity)


def parse_lista_reponer(s: str):
    if s is None:
        return []
    txt = str(s).strip()
    if not txt or txt.lower() in {"nan", "none"}:
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


def _pick_qty_col(rep_df: pd.DataFrame) -> str | None:
    """
    Detecta la columna que representa "cantidad a reponer" hacia máximo.
    Esto evita depender de nombres exactos en src.normalize.
    """
    if rep_df is None or rep_df.empty:
        return None

    blacklist = {"min", "max", "stock", "exist", "cantidad_stock", "cantidad_stock_real"}
    candidates = []
    for c in rep_df.columns:
        lc = str(c).strip().lower()
        if lc in blacklist:
            continue
        if any(x in lc for x in ["to_max", "reponer", "need", "qty", "a_reponer", "replen", "faltan", "faltante"]):
            candidates.append(c)

    # Si hay candidatos "semánticos", probamos el que más suma positiva tenga.
    def score(col):
        try:
            s = pd.to_numeric(rep_df[col], errors="coerce").fillna(0)
            return float(s[s > 0].sum())
        except Exception:
            return 0.0

    if candidates:
        best = sorted(candidates, key=score, reverse=True)[0]
        if score(best) > 0:
            return best

    # Fallback: primera numérica que tenga suma positiva relevante y no sea Min/Max/Stock
    numeric_cols = []
    for c in rep_df.columns:
        if c in {"ALMACEN", "Amenity", "AmenityKey"}:
            continue
        try:
            s = pd.to_numeric(rep_df[c], errors="coerce")
            if s.notna().any():
                numeric_cols.append(c)
        except Exception:
            pass

    if not numeric_cols:
        return None

    best = sorted(numeric_cols, key=score, reverse=True)[0]
    return best if score(best) > 0 else None


def _rep_to_text_by_almacen(
    rep_df: pd.DataFrame,
    cafe_tipo_by_apto: dict[str, str],
    apt_to_alm: dict[str, str],
    master_amenities: set[str] | None = None,
):
    """
    Construye texto COMPLETO (sin recortes) desde el dataframe de reposición por almacén.
    Filtra el café según CAFE_TIPO del apartamento, para evitar mezclar Tassimo/Nespresso/Molido.
    """
    out_lista = {}  # (apto)->texto
    if rep_df is None or rep_df.empty:
        return out_lista

    qty_col = _pick_qty_col(rep_df)
    if qty_col is None:
        return out_lista

    # Detecta columna de amenity "humana"
    amen_col = None
    for c in ["Amenity", "AmenityName", "AmenityLabel"]:
        if c in rep_df.columns:
            amen_col = c
            break
    if amen_col is None:
        # fallback: si no existe, usamos AmenityKey
        for c in ["AmenityKey", "Amenity_key", "amenity_key"]:
            if c in rep_df.columns:
                amen_col = c
                break
    if amen_col is None:
        return out_lista

    rep = rep_df.copy()
    rep["__qty"] = pd.to_numeric(rep[qty_col], errors="coerce").fillna(0).astype(int)
    rep = rep[rep["__qty"] > 0].copy()
    if rep.empty:
        return out_lista

    rep["__amen"] = rep[amen_col].astype(str)

    # Normalización contra maestro (si existe) para evitar "Inse", "Det", etc.
    # Nota: aquí no “inventamos” amenities; solo forzamos a los del maestro si encajan por prefijo.
    if master_amenities:
        master_keys = { _apt_key(x): x for x in master_amenities }

        def normalize_to_master(x: str) -> str:
            k = _apt_key(x)
            if k in master_keys:
                return master_keys[k]
            # prefijo único (ej: INSE -> INSECTICIDA)
            hits = [v for kk, v in master_keys.items() if kk.startswith(k) and k]
            if len(hits) == 1:
                return hits[0]
            # prefijo inverso (ej: AZUCAR BLANCO... si viene más largo/variado)
            hits2 = [v for kk, v in master_keys.items() if k.startswith(kk) and kk]
            if len(hits2) == 1:
                return hits2[0]
            return x

        rep["__amen_norm"] = rep["__amen"].map(normalize_to_master)
    else:
        rep["__amen_norm"] = rep["__amen"]

    # Construye por ALMACEN una lista (amenity, qty)
    by_alm = {}
    for alm, g in rep.groupby("ALMACEN"):
        items = []
        for _, r in g.iterrows():
            items.append((str(r["__amen_norm"]), int(r["__qty"])))
        by_alm[str(alm).strip()] = items

    # Ahora para cada apartamento, filtra café según su tipo, y aplica DISPLAY_MAP
    for apt, alm in apt_to_alm.items():
        alm = str(alm).strip()
        items = by_alm.get(alm, [])
        if not items:
            continue

        cafe_tipo = str(cafe_tipo_by_apto.get(apt, "")).strip()
        cafe_key = COFFEE_BY_TIPO.get(cafe_tipo, None)  # amenity key normalizada del café permitido

        built = []
        for amen, qty in items:
            ak = _apt_key(amen)

            # Si es uno de los 3 cafés, solo incluir el que toca para ese apto
            if ak in { _apt_key("Capsula Tassimo"), _apt_key("Café en cápsula Colombia"), _apt_key("Café Natural Molido") }:
                if cafe_key is None or ak != cafe_key:
                    continue

            label = _display_name(amen)
            built.append(f"{label} x{qty}")

        out_lista[apt] = ", ".join(built)

    return out_lista


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
            if str(txt).strip() == "" or str(txt).strip().lower() in {"nan", "none"}:
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
# Visor SIN HTML para texto completo (por fila)
# =========================
def show_full_text_viewer(df: pd.DataFrame, title: str, key_prefix: str):
    """
    Visor práctico: seleccionas 1 fila y te muestra el texto completo en text_area.
    IMPORTANTE: keys únicas para evitar StreamlitDuplicateElementId.
    """
    if df is None or df.empty:
        st.info("Sin datos para mostrar.")
        return

    cols_txt = [c for c in ["Lista_reponer", "Completar con"] if c in df.columns]
    if not cols_txt:
        st.info("No hay columnas de texto (Lista_reponer / Completar con).")
        return

    with st.expander(f"🔎 Texto completo ({title})", expanded=False):
        st.caption("Selecciona una fila para ver el texto completo (sin cortes).")

        # Opciones legibles
        def _row_label(r):
            dia = r.get("Día", "")
            apt = r.get("APARTAMENTO", "")
            cli = r.get("Cliente", "")
            est = r.get("Estado", "")
            return f"{dia} · {apt} · {cli} · {est}".strip(" ·")

        df2 = df.copy().reset_index(drop=True)
        options = list(range(len(df2)))
        labels = [_row_label(df2.loc[i]) for i in options]

        sel = st.selectbox(
            "Fila",
            options=options,
            format_func=lambda i: labels[i] if i < len(labels) else str(i),
            key=f"{key_prefix}_row_select",
        )
        row = df2.loc[int(sel)]

        for c in cols_txt:
            st.text_area(
                f"{c} (completo)",
                value="" if pd.isna(row.get(c)) else str(row.get(c)),
                height=140,
                key=f"{key_prefix}_{c}_{int(sel)}",  # <-- clave única real
            )


def _kpi_table(df: pd.DataFrame, title: str, key_prefix: str):
    if df is None or df.empty:
        st.info("Sin resultados.")
        return

    cols_show = [c for c in ["Día", "ZONA", "APARTAMENTO", "Cliente", "Estado", "CAFE_TIPO", "Lista_reponer", "Completar con", "Próxima Entrada"] if c in df.columns]
    view = df[cols_show].reset_index(drop=True).copy()

    st.markdown(f"#### {title}")
    _render_operativa_table(view, key=f"{key_prefix}_table", styled=False)
    show_full_text_viewer(view, title=title, key_prefix=f"{key_prefix}_fulltext")


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

    # Normaliza APARTAMENTO
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

    ap_map = ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]].dropna(subset=["APARTAMENTO", "ALMACEN"]).drop_duplicates()
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

    # =========================
    # 🔥 FIX CLAVE: RECONSTRUIR Lista_reponer / Completar con SIN RECORTES
    # (evita "Inse", "Det", "Gel d"...)
    # =========================
    oper_tmp = dash["operativa"].copy()

    # map apto->almacen
    apt_to_alm = {}
    if "APARTAMENTO" in oper_tmp.columns and "ALMACEN" in oper_tmp.columns:
        for _, r in oper_tmp[["APARTAMENTO", "ALMACEN"]].dropna().drop_duplicates().iterrows():
            apt_to_alm[str(r["APARTAMENTO"]).strip()] = str(r["ALMACEN"]).strip()

    # map apto->cafe_tipo
    cafe_tipo_by_apto = {}
    if "APARTAMENTO" in oper_tmp.columns and "CAFE_TIPO" in oper_tmp.columns:
        for _, r in oper_tmp[["APARTAMENTO", "CAFE_TIPO"]].dropna().drop_duplicates().iterrows():
            cafe_tipo_by_apto[str(r["APARTAMENTO"]).strip()] = str(r["CAFE_TIPO"]).strip()

    # maestro amenities (para normalizar)
    master_amenities = set()
    try:
        th = masters.get("thresholds")
        if th is not None and not th.empty:
            # intenta sacar nombre humano
            for c in ["Amenity", "AmenityName", "Producto", "AmenityLabel"]:
                if c in th.columns:
                    master_amenities = set(th[c].dropna().astype(str).tolist())
                    break
    except Exception:
        master_amenities = set()

    # lista urgente (si aplica) y lista completa
    urgent_text_by_ap = _rep_to_text_by_almacen(rep, cafe_tipo_by_apto, apt_to_alm, master_amenities=master_amenities)
    all_text_by_ap = _rep_to_text_by_almacen(rep_all, cafe_tipo_by_apto, apt_to_alm, master_amenities=master_amenities)

    # aplica en operativa:
    # - En modo normal: "Lista_reponer" = all_text
    # - En modo URGENTE: "Lista_reponer" = urgent_text y "Completar con" = all_text
    if urgent_only:
        oper_tmp["Lista_reponer"] = oper_tmp["APARTAMENTO"].map(lambda a: urgent_text_by_ap.get(str(a).strip(), ""))
        oper_tmp["Completar con"] = oper_tmp["APARTAMENTO"].map(lambda a: all_text_by_ap.get(str(a).strip(), ""))
    else:
        oper_tmp["Lista_reponer"] = oper_tmp["APARTAMENTO"].map(lambda a: all_text_by_ap.get(str(a).strip(), ""))
        if "Completar con" not in oper_tmp.columns:
            oper_tmp["Completar con"] = ""

    dash["operativa"] = oper_tmp  # <-- reemplazo definitivo

    # =========================
    # ✅ DASHBOARD ARRIBA + "CLICK" (botones) para ver listados
    # =========================
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
            _kpi_table(df, f"Entradas · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}", key_prefix="kpi_entradas")

        elif kpi_open == "salidas":
            df = oper_foco[oper_foco["Estado"].isin(["SALIDA", "ENTRADA+SALIDA"])].copy()
            _kpi_table(df, f"Salidas · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}", key_prefix="kpi_salidas")

        elif kpi_open == "turnovers":
            df = oper_foco[oper_foco["Estado"].isin(["ENTRADA+SALIDA"])].copy()
            _kpi_table(df, f"Turnovers · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}", key_prefix="kpi_turnovers")

        elif kpi_open == "ocupados":
            df = oper_foco[oper_foco["Estado"].isin(["OCUPADO"])].copy()
            _kpi_table(df, f"Ocupados · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}", key_prefix="kpi_ocupados")

        elif kpi_open == "vacios":
            df = oper_foco[oper_foco["Estado"].isin(["VACIO"])].copy()
            _kpi_table(df, f"Vacíos · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}", key_prefix="kpi_vacios")

        elif kpi_open == "presenciales":
            _kpi_table(pres_today, f"Check-ins presenciales · HOY {pd.to_datetime(today).strftime('%d/%m/%Y')}", key_prefix="kpi_presenciales")

    # =========================
    # 🔎 BUSCADOR PRINCIPAL (Limpieza + Operativa + Reposición)
    # =========================
    st.divider()
    st.subheader("🔎 Buscar apartamento · Resumen (Limpieza + Operativa + Reposición)")

    if "apt_query" not in st.session_state:
        st.session_state["apt_query"] = ""
    if "apt_selected_key" not in st.session_state:
        st.session_state["apt_selected_key"] = ""

    st.text_input(
        "Escribe el apartamento (o parte) y pulsa Enter",
        key="apt_query",
        placeholder="Ej: APOLO 29, BENICALAP, ALMIRANTE...",
    )

    if st.button("Buscar", key="btn_buscar_apto"):
        st.session_state["apt_selected_key"] = _apt_key(st.session_state["apt_query"])

    apt_key_sel = st.session_state.get("apt_selected_key", "").strip()

    last_view = pd.DataFrame()
    try:
        sheet_df = read_sheet_df()
        if sheet_df is not None and not sheet_df.empty:
            last_view = build_last_report_view(sheet_df)
            last_view["APARTAMENTO_KEY"] = last_view["Apartamento"].map(_apt_key)
    except Exception as e:
        st.warning("No pude construir el último informe por apartamento desde Google Sheet.")
        st.exception(e)

    if apt_key_sel:
        st.markdown("### 🧹 Última limpieza (según Marca temporal)")
        if last_view is None or last_view.empty:
            st.info("No hay datos de limpieza disponibles.")
        else:
            one = last_view[last_view["APARTAMENTO_KEY"] == apt_key_sel].copy()
            if one.empty:
                st.info("No encuentro último informe para ese apartamento en la Sheet.")
            else:
                show_cols = ["Apartamento", "Último informe", "LLAVES", "OTRAS REPOSICIONES", "INCIDENCIAS/TAREAS A REALIZAR"]
                show_cols = [c for c in show_cols if c in one.columns]
                st.dataframe(one[show_cols].reset_index(drop=True), use_container_width=True)

        st.markdown("### 🧾 Parte Operativo (solo este apartamento)")
        op_one = oper_all[oper_all["APARTAMENTO_KEY"] == apt_key_sel].copy()
        if op_one.empty:
            st.info("No hay filas de operativa para ese apartamento en el periodo seleccionado.")
        else:
            if zonas_sel:
                op_one = op_one[op_one["ZONA"].isin(zonas_sel)].copy()
            if estados_sel:
                op_one = op_one[op_one["Estado"].isin(estados_sel)].copy()

            op_one = op_one.sort_values(["Día", "ZONA", "__prio", "APARTAMENTO"], ascending=[True, True, True, True])
            op_show = op_one.drop(columns=["APARTAMENTO_KEY"], errors="ignore").copy()
            _render_operativa_table(op_show, key="buscador_operativa", styled=True)

            # visor texto completo por fila (aquí sí tiene sentido)
            show_full_text_viewer(op_show, title="Buscador", key_prefix="buscador_fulltext")
    else:
        st.caption("Escribe un apartamento y pulsa Enter o el botón Buscar. (No se muestra nada por defecto.)")

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

    operativa = dash["operativa"].copy()
    operativa["APARTAMENTO_KEY"] = operativa["APARTAMENTO"].map(_apt_key)

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
            _render_operativa_table(show_df, key=f"parte_{_apt_key(str(dia))}_{_apt_key(str(zona_label))}", styled=True)

            # visor texto completo para esa tabla zona+día
            show_full_text_viewer(show_df, title=f"{pd.to_datetime(dia).strftime('%d/%m/%Y')} · {zona_label}", key_prefix=f"full_{_apt_key(str(dia))}_{_apt_key(str(zona_label))}")

    # =========================
    # SUGERENCIA DE REPOSICIÓN (totales)
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
            st.dataframe(totals_df, use_container_width=True, height=_safe_height(len(totals_df)))
        with colB:
            st.markdown("**Dónde dejar cada producto** (por ZONA y APARTAMENTO)**")
            st.dataframe(items_df, use_container_width=True, height=_safe_height(min(len(items_df), 25)))

    # =========================
    # RUTAS GOOGLE MAPS
    # =========================
    st.divider()
    st.subheader("📍 Ruta Google Maps · Reposición HOY + MAÑANA (por ZONA)")
    st.caption("Criterio: con reposición y Estado == VACIO o ENTRADA o ENTRADA+SALIDA ese día. Botones directos a Maps.")

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
        try:
            st.dataframe(rep.sort_values(["ALMACEN"], na_position="last").reset_index(drop=True), use_container_width=True)
        except Exception:
            st.dataframe(rep.reset_index(drop=True), use_container_width=True)

        if not unclassified.empty:
            st.warning("Hay productos sin clasificar (no entran en reposición).")
            st.dataframe(unclassified.reset_index(drop=True), use_container_width=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
