import streamlit as st
import pandas as pd
from zoneinfo import ZoneInfo
from urllib.parse import quote
import re
import unicodedata


ORIGIN_LAT = 39.45702028460933
ORIGIN_LNG = -0.38498336081567713


# =========================
# PAX MAX (definido en código, como pediste)
# =========================
PAX_MAX_BY_APT = {
    "ALFARO": 5,
    "ALMIRANTE 01": 4,
    "ALMIRANTE 02": 4,
    "APOLO 029": 2,
    "APOLO 180": 3,
    "APOLO 197": 4,
    "BENICALAP 01": 4,
    "BENICALAP 02": 4,
    "BENICALAP 03": 3,
    "BENICALAP 05": 4,
    "BENICALAP 06": 4,
    "CADIZ": 4,
    "CARCAIXENT 01": 4,
    "CARCAIXENT 02": 4,
    "DENIA 61": 4,
    "DOLORES ALCAYDE 01": 4,
    "DOLORES ALCAYDE 02": 4,
    "DOLORES ALCAYDE 03": 4,
    "DOLORES ALCAYDE 04": 4,
    "DOLORES ALCAYDE 05": 4,
    "DOLORES ALCAYDE 06": 4,
    "DR.LLUCH": 4,
    "ERUDITO": 4,
    "GOZALBO": 4,
    "LA ELIANA": 5,
    "LLADRO Y MALLI 01": 5,
    "LLADRO Y MALLI 02": 7,
    "LLADRO Y MALLI 03": 5,
    "LLADRO Y MALLI 04": 7,
    "LLADRO Y MALLI BAJO A": 5,
    "LLADRO Y MALLI BAJO B": 4,
    "LUIS MERELO 01": 2,
    "LUIS MERELO 02": 2,
    "LUIS MERELO 03": 4,
    "LUIS MERELO 04": 4,
    "LUIS MERELO 05": 4,
    "LUIS MERELO 06": 4,
    "LUIS MERELO 07": 4,
    "LUIS MERELO 08": 4,
    "LUIS MERELO 09": 4,
    "MARIANO CUBER 01": 2,
    "MARIANO CUBER 02": 3,
    "MARIANO CUBER 03": 2,
    "MARIANO CUBER 04": 4,
    "MARIANO CUBER 05": 4,
    "OLIVERETA 1": 4,
    "OLIVERETA 2": 4,
    "OLIVERETA 3": 4,
    "OLIVERETA 4": 4,
    "OVE 01": 4,
    "OVE 02": 4,
    "PADRE PORTA 01": 4,
    "PADRE PORTA 02": 4,
    "PADRE PORTA 03": 6,
    "PADRE PORTA 04": 3,
    "PADRE PORTA 05": 4,
    "PADRE PORTA 06": 6,
    "PADRE PORTA 07": 6,
    "PADRE PORTA 08": 4,
    "PADRE PORTA 09": 4,
    "PADRE PORTA 10": 4,
    "PASAJE AYF 01": 4,
    "PASAJE AYF 02": 4,
    "PASAJE AYF 03": 4,
    "PINTOR": 3,
    "QUART I": 4,
    "QUART II": 4,
    "RETOR A": 4,
    "RETOR B": 4,
    "SAN LUIS": 6,
    "SERRANOS": 4,
    "SERRERIA 01": 6,
    "SERRERIA 02": 4,
    "SERRERIA 03": 4,
    "SERRERIA 04": 4,
    "SERRERIA 05": 4,
    "SERRERIA 06": 4,
    "SERRERIA 07": 3,
    "SERRERIA 08": 3,
    "SERRERIA 09": 4,
    "SERRERIA 10": 4,
    "SERRERIA 11": 4,
    "SERRERIA 12": 4,
    "SERRERIA 13": 4,
    "SEVILLA": 2,
    "TRAFALGAR 01": 4,
    "TRAFALGAR 02": 4,
    "TRAFALGAR 03": 4,
    "TRAFALGAR 04": 2,
    "TRAFALGAR 05": 4,
    "TRAFALGAR 06": 4,
    "TRAFALGAR 07": 4,
    "TUNDIDORES": 4,
    "VALLE": 4,
    "VISITACION": 6,
    "ZAPATEROS 10-2": 4,
    "ZAPATEROS 10-6": 4,
    "ZAPATEROS 10-8": 5,
    "ZAPATEROS 12-5": 2,
}


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


def _pax_max_for(apt: str) -> int | None:
    k = _apt_key(apt)
    return PAX_MAX_BY_APT.get(k, None)


# =========================
# Disponibilidad (TELÉFONO)
# Regla: disponible si VACIO todas las noches [CI .. CO-1]
# =========================
def _available_apartments_from_operativa(
    operativa_df: pd.DataFrame,
    check_in: pd.Timestamp,
    check_out: pd.Timestamp,
) -> pd.DataFrame:
    if operativa_df is None or operativa_df.empty:
        return pd.DataFrame(columns=["APARTAMENTO"])

    need = {"Día", "APARTAMENTO", "Estado"}
    if not need.issubset(operativa_df.columns):
        return pd.DataFrame(columns=["APARTAMENTO"])

    df = operativa_df.copy()
    df["APARTAMENTO"] = df["APARTAMENTO"].astype(str).str.strip()
    df["APARTAMENTO_KEY"] = df["APARTAMENTO"].map(_apt_key)
    df["Día"] = pd.to_datetime(df["Día"], errors="coerce").dt.date
    df["Estado"] = df["Estado"].astype(str).str.strip().str.upper()

    ci = pd.to_datetime(check_in).normalize().date()
    co = pd.to_datetime(check_out).normalize().date()
    if co <= ci:
        return pd.DataFrame(columns=["APARTAMENTO"])

    last_night = (pd.to_datetime(check_out).normalize() - pd.Timedelta(days=1)).date()

    noches = []
    cur = pd.to_datetime(ci)
    end = pd.to_datetime(last_night)
    while cur <= end:
        noches.append(cur.date())
        cur += pd.Timedelta(days=1)

    df_n = df[df["Día"].isin(noches)].copy()
    if df_n.empty:
        return pd.DataFrame(columns=["APARTAMENTO"])

    g = df_n.groupby("APARTAMENTO_KEY")["Estado"].apply(list).reset_index(name="estados")

    def _all_vacio(estados):
        return bool(estados) and all(e == "VACIO" for e in estados)

    g["is_free"] = g["estados"].apply(_all_vacio)
    free_keys = set(g[g["is_free"]]["APARTAMENTO_KEY"].tolist())

    # Devolvemos nombre “bonito” (primera ocurrencia del periodo)
    one_name = (
        df[["APARTAMENTO_KEY", "APARTAMENTO"]]
        .dropna()
        .drop_duplicates(subset=["APARTAMENTO_KEY"])
        .copy()
    )
    out = one_name[one_name["APARTAMENTO_KEY"].isin(free_keys)][["APARTAMENTO"]].copy()
    out = out.sort_values("APARTAMENTO").reset_index(drop=True)
    return out


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


def main():
    from src.loaders import load_masters_repo
    from src.parsers import parse_avantio_entradas, parse_odoo_stock
    from src.normalize import normalize_products, summarize_replenishment
    from src.dashboard import build_dashboard_frames
    from src.gsheets import read_sheet_df

    # build_last_report_view puede estar en src/cleaning_last_report.py o src/parsers/cleaning_last_report.py
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

    # Operativa global (para reusar)
    oper_all = dash["operativa"].copy()
    oper_all["APARTAMENTO_KEY"] = oper_all["APARTAMENTO"].map(_apt_key)

    # =========================
    # KPIs dashboard + Check-ins presenciales
    # =========================
    kpis = dash.get("kpis", {})
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Entradas (día foco)", kpis.get("entradas_dia", 0))
    c2.metric("Salidas (día foco)", kpis.get("salidas_dia", 0))
    c3.metric("Turnovers", kpis.get("turnovers_dia", 0))
    c4.metric("Ocupados", kpis.get("ocupados_dia", 0))
    c5.metric("Vacíos", kpis.get("vacios_dia", 0))

    tz = ZoneInfo("Europe/Madrid")
    today = pd.Timestamp.now(tz=tz).normalize().date()

    presencial_set = {"APOLO 029", "APOLO 180", "APOLO 197", "SERRANOS"}
    presencial_keys = {_apt_key(x) for x in presencial_set}

    oper_today = oper_all.copy()
    oper_today["Día"] = pd.to_datetime(oper_today["Día"], errors="coerce").dt.date

    pres_today = oper_today[
        (oper_today["Día"] == today)
        & (oper_today["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA"]))
        & (oper_today["APARTAMENTO_KEY"].isin(presencial_keys))
    ].copy()

    c6.metric("Check-ins presenciales (HOY)", int(len(pres_today)))

    if len(pres_today) > 0:
        with st.expander("Ver check-ins presenciales (HOY)", expanded=False):
            cols_show = [c for c in ["Día", "ZONA", "APARTAMENTO", "Cliente", "Estado"] if c in pres_today.columns]
            st.dataframe(pres_today[cols_show].reset_index(drop=True), use_container_width=True)

    # =========================
    # 🔎 BUSCADOR TELÉFONO (Disponibilidad)
    # =========================
    st.divider()
    st.subheader("📞 Búsqueda rápida (Teléfono) · Disponibilidad por fechas + Zona + PAX")

    tcol1, tcol2, tcol3, tcol4 = st.columns([1, 1, 1, 1.2])
    with tcol1:
        tel_ci = st.date_input("Entrada", value=pd.Timestamp.today().date(), key="tel_ci")
    with tcol2:
        tel_co = st.date_input("Salida", value=(pd.Timestamp.today() + pd.Timedelta(days=2)).date(), key="tel_co")
    with tcol3:
        pax_min = st.number_input("PAX mínimo", min_value=1, max_value=20, value=2, step=1, key="tel_pax")
    with tcol4:
        zona_opts = ["Todas"] + (zonas_all if isinstance(zonas_all, list) else [])
        tel_zona = st.selectbox("Zona", options=zona_opts, index=0, key="tel_zona")

    do_tel = st.button("Buscar disponibles", key="tel_btn")

    if do_tel:
        ci = pd.Timestamp(tel_ci)
        co = pd.Timestamp(tel_co)

        if co <= ci:
            st.error("La salida debe ser posterior a la entrada.")
        else:
            # Recalcular un periodo que cubra exactamente las NOCHES del rango (no hace falta buffer)
            nights = int((co - ci).days)
            try:
                dash_tel = build_dashboard_frames(
                    avantio_df=avantio_df,
                    replenishment_df=rep,
                    rep_all_df=rep_all,
                    urgent_only=urgent_only,
                    unclassified_products=unclassified,
                    period_start=tel_ci,
                    period_days=max(1, nights),
                )
                oper_tel = dash_tel["operativa"].copy()
            except Exception as e:
                st.warning("No pude recalcular para el rango; uso el periodo actual.")
                st.exception(e)
                oper_tel = oper_all.copy()

            free_apts = _available_apartments_from_operativa(oper_tel, ci, co)
            if free_apts.empty:
                st.info("No hay apartamentos totalmente VACÍOS para esas noches (según entradas/operativa).")
            else:
                out = free_apts.copy()
                out["APARTAMENTO_KEY"] = out["APARTAMENTO"].map(_apt_key)
                out["PAX_MAX"] = out["APARTAMENTO_KEY"].map(lambda k: PAX_MAX_BY_APT.get(k, None))

                # Zona desde maestro
                if "zonas" in masters and "APARTAMENTO" in masters["zonas"].columns:
                    tmp_z = masters["zonas"][["APARTAMENTO", "ZONA"]].copy()
                    tmp_z["APARTAMENTO_KEY"] = tmp_z["APARTAMENTO"].map(_apt_key)
                    out = out.merge(tmp_z[["APARTAMENTO_KEY", "ZONA"]], on="APARTAMENTO_KEY", how="left")

                if tel_zona != "Todas" and "ZONA" in out.columns:
                    out = out[out["ZONA"] == tel_zona].copy()

                out["PAX_MAX"] = pd.to_numeric(out["PAX_MAX"], errors="coerce")
                out = out[out["PAX_MAX"].fillna(0) >= pax_min].copy()

                out = out.sort_values(["ZONA", "APARTAMENTO"], na_position="last").reset_index(drop=True)
                st.success(f"Disponibles: {len(out)}")
                show_cols = [c for c in ["APARTAMENTO", "ZONA", "PAX_MAX"] if c in out.columns]
                st.dataframe(out[show_cols], use_container_width=True, height=min(520, 40 + 35 * min(len(out), 25)))

    # =========================
    # 🔎 BUSCADOR PRINCIPAL (Limpieza + Operativa + Reposición) con dropdown
    # =========================
    st.divider()
    st.subheader("🔎 Buscar apartamento · Resumen (Limpieza + Operativa + Reposición)")

    # Estado UI: vacío por defecto
    if "apt_query" not in st.session_state:
        st.session_state["apt_query"] = ""
    if "apt_choice" not in st.session_state:
        st.session_state["apt_choice"] = ""
    if "apt_selected_key" not in st.session_state:
        st.session_state["apt_selected_key"] = ""

    # Cargar sheet + construir último informe por apto (una vez por ejecución)
    last_view = pd.DataFrame()
    try:
        sheet_df = read_sheet_df()
        if sheet_df is not None and not sheet_df.empty:
            last_view = build_last_report_view(sheet_df)
            last_view["APARTAMENTO_KEY"] = last_view["Apartamento"].map(_apt_key)
    except Exception as e:
        st.warning("No pude construir el último informe por apartamento desde Google Sheet.")
        st.exception(e)

    # Universo de apartamentos (operativa + sheet + pax)
    apts_from_oper = sorted(set(oper_all["APARTAMENTO"].dropna().astype(str).str.strip().tolist())) if not oper_all.empty else []
    apts_from_sheet = sorted(set(last_view["Apartamento"].dropna().astype(str).str.strip().tolist())) if not last_view.empty else []
    apts_from_pax = sorted(PAX_MAX_BY_APT.keys())

    universe = sorted({_apt_key(x) for x in (apts_from_oper + apts_from_sheet + apts_from_pax) if str(x).strip()})
    # Para mostrar nombres, preferimos el “bonito” que venga del propio texto original:
    # construimos un mapping key -> display (primera coincidencia razonable)
    display_map = {}
    for name in apts_from_oper + apts_from_sheet + apts_from_pax:
        k = _apt_key(name)
        if k and k not in display_map:
            display_map[k] = _apt_key(name)  # display en MAYUS normalizada

    query = st.text_input(
        "Escribe (o parte) y pulsa Enter. Luego elige en el desplegable y pulsa Aplicar.",
        key="apt_query",
        placeholder="Ej: SERRANOS, APOLO, BENICALAP, ALMIRANTE...",
    ).strip()

    matches = []
    if query:
        qk = _apt_key(query)
        # matching por “contiene”
        for k in universe:
            if qk in k:
                matches.append(display_map.get(k, k))
        matches = sorted(set(matches))

    if matches:
        st.selectbox("Coincidencias", options=[""] + matches, key="apt_choice")
        if st.button("Aplicar", key="apt_apply"):
            st.session_state["apt_selected_key"] = _apt_key(st.session_state.get("apt_choice", ""))
    else:
        st.caption("Escribe para ver coincidencias. (Nada se muestra por defecto.)")

    apt_key_sel = st.session_state.get("apt_selected_key", "").strip()

    if apt_key_sel:
        st.markdown(f"## ✅ Resumen: `{display_map.get(apt_key_sel, apt_key_sel)}`")

        # ===== Limpieza (último informe) =====
        st.markdown("### 🧹 Última limpieza (Marca temporal)")
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

        # ===== Operativa =====
        st.markdown("### 🧾 Parte Operativo (solo este apartamento)")
        op_one = oper_all[oper_all["APARTAMENTO_KEY"] == apt_key_sel].copy()
        if op_one.empty:
            st.info("No hay filas de operativa para ese apartamento en el periodo seleccionado.")
        else:
            # Aplica filtros de sidebar
            if zonas_sel and "ZONA" in op_one.columns:
                op_one = op_one[op_one["ZONA"].isin(zonas_sel)].copy()
            if estados_sel and "Estado" in op_one.columns:
                op_one = op_one[op_one["Estado"].isin(estados_sel)].copy()

            # orden seguro (si faltan columnas internas, no peta)
            sort_cols = [c for c in ["Día", "ZONA", "__prio", "APARTAMENTO"] if c in op_one.columns]
            if sort_cols:
                op_one = op_one.sort_values(sort_cols, ascending=True)

            op_show = op_one.drop(columns=["APARTAMENTO_KEY"], errors="ignore").copy()
            st.dataframe(_style_operativa(op_show), use_container_width=True)

        # ===== Reposición (solo apto) =====
        st.markdown("### 📦 Reposición (solo este apartamento)")
        if op_one.empty:
            st.info("Sin reposición (no hay operativa para este apartamento).")
        else:
            cols_rep = [c for c in ["Lista_reponer", "Completar con"] if c in op_one.columns]
            if not cols_rep:
                st.info("No veo columnas de reposición en la operativa para este apartamento.")
            else:
                rep_rows = op_one[["Día", "ZONA", "APARTAMENTO"] + cols_rep].copy() if "ZONA" in op_one.columns else op_one[["Día", "APARTAMENTO"] + cols_rep].copy()
                for c in cols_rep:
                    rep_rows[c] = rep_rows[c].astype(str).fillna("").str.strip()

                def _has_any_rep(r):
                    for c in cols_rep:
                        v = str(r.get(c, "")).strip()
                        if v and v.lower() not in {"nan", "none"}:
                            return True
                    return False

                rep_rows = rep_rows[rep_rows.apply(_has_any_rep, axis=1)].copy()
                if rep_rows.empty:
                    st.info("No hay reposición indicada para este apartamento en el periodo.")
                else:
                    st.dataframe(rep_rows.reset_index(drop=True), use_container_width=True)

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

    operativa = operativa.sort_values([c for c in ["Día", "ZONA", "__prio", "APARTAMENTO"] if c in operativa.columns])

    for dia, ddf in operativa.groupby("Día", dropna=False):
        st.markdown(f"### Día {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
        if ddf.empty:
            st.info("Sin datos.")
            continue

        for zona, zdf in ddf.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
            st.markdown(f"#### {zona_label}")
            show_df = zdf.drop(columns=["ZONA", "__prio", "APARTAMENTO_KEY"], errors="ignore").copy()
            st.dataframe(
                _style_operativa(show_df),
                use_container_width=True,
                height=min(520, 40 + 35 * len(show_df)),
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
            st.dataframe(totals_df, use_container_width=True, height=min(520, 40 + 35 * len(totals_df)))
        with colB:
            st.markdown("**Dónde dejar cada producto** (por ZONA y APARTAMENTO)")
            st.dataframe(items_df, use_container_width=True, height=min(520, 40 + 28 * min(len(items_df), 25)))

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

    st.download_button(
        "⬇️ Descargar Excel (Operativa)",
        data=dash["excel_all"],
        file_name=dash["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

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
