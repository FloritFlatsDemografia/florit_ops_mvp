import streamlit as st
import pandas as pd
from zoneinfo import ZoneInfo
from urllib.parse import quote


# =========================
# Config ruta (Google Maps)
# =========================
ORIGIN_LAT = 39.45702028460933
ORIGIN_LNG = -0.38498336081567713


def _coord_str(lat, lng):
    try:
        return f"{float(lat):.8f},{float(lng):.8f}"
    except Exception:
        return None


def build_gmaps_directions_url(coords, travelmode="walking", return_to_base=False):
    """
    coords: lista de strings "lat,lng" (paradas).
    - return_to_base=True: destination = origen, waypoints = paradas
    - return_to_base=False: destination = última parada, waypoints = resto
    """
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
# Parseo robusto de coords
# =========================
def parse_lat_lng(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Acepta:
      - "39.49,-0.39"
      - "39.49; -0.39"
      - "39.49 -0.39"
      - "(39.49, -0.39)"
    Devuelve LAT y LNG numéricos (NaN si no se puede parsear).
    """
    s = series.astype(str).str.strip()
    s = s.str.replace("(", "", regex=False).str.replace(")", "", regex=False)

    # extrae dos floats separados por coma / ; / espacio(s)
    ext = s.str.extract(r"([+-]?\d+(?:\.\d+)?)\s*[,; ]\s*([+-]?\d+(?:\.\d+)?)")
    lat = pd.to_numeric(ext[0], errors="coerce")
    lng = pd.to_numeric(ext[1], errors="coerce")
    return lat, lng


# =========================
# Estilos tabla operativa
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
# BOOTSTRAP
# =========================
st.set_page_config(page_title="Florit OPS – Operativa & Reposición", layout="wide")
st.title("Florit OPS – Parte diario (Operativa + Reposición)")
st.caption("Si ves esto, el script está arrancando. Si se queda en blanco, no está ejecutando app.py.")


def main():
    # Imports “pesados” después de pintar
    try:
        from src.loaders import load_masters_repo
        from src.parsers import parse_avantio_entradas, parse_odoo_stock
        from src.normalize import normalize_products, summarize_replenishment
        from src.dashboard import build_dashboard_frames
    except Exception as e:
        st.error("Error importando módulos de /src (loaders/parsers/normalize/dashboard).")
        st.exception(e)
        st.stop()

    with st.expander("📌 Cómo usar", expanded=False):
        st.markdown(
            """
**2 clics:**
1) Subes Avantio
2) Subes Odoo

📌 Maestros desde `data/` (GitHub):
- Zonas
- Apt↔Almacén (+ Localizacion)
- Café por apartamento
- Stock mínimo/máximo (thresholds)

📍 Ruta Google Maps: reposición HOY y MAÑANA por ZONA (salida Florit Flats).
"""
        )

    # Sidebar
    st.sidebar.header("Archivos diarios")
    avantio_file = st.sidebar.file_uploader(
        "Avantio (Entradas) .xls/.xlsx/.csv",
        type=["xls", "xlsx", "csv", "html"],
    )
    odoo_file = st.sidebar.file_uploader(
        "Odoo (stock.quant) .xlsx/.csv",
        type=["xlsx", "csv"],
    )

    tz = ZoneInfo("Europe/Madrid")
    today = pd.Timestamp.now(tz=tz).normalize().date()

    with st.sidebar.expander("Avanzado (opcional)", expanded=False):
        period_start = st.date_input("Inicio", value=today)
        period_days = st.number_input("Nº días", min_value=1, max_value=14, value=2, step=1)
        only_replenishment = st.checkbox("Mostrar SOLO apartamentos con reposición", value=True)
        travelmode = st.selectbox("Modo ruta", ["walking", "driving"], index=0)
        return_to_base = st.checkbox("Volver a Florit Flats al final", value=False)

    # Maestros
    try:
        with st.spinner("Cargando maestros (data/ en GitHub)…"):
            masters = load_masters_repo()
        st.sidebar.success("Maestros cargados ✅")
    except Exception as e:
        st.error("Fallo cargando maestros (data/).")
        st.exception(e)
        st.stop()

    if not (avantio_file and odoo_file):
        st.info("Sube Avantio + Odoo para generar el parte operativo.")
        st.stop()

    # Parse inputs
    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)

    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant (archivo vacío o columnas no detectadas).")
        st.stop()

    # Normaliza Odoo
    odoo_norm = normalize_products(odoo_df)

    # Maestro apt_almacen + localización
    apt_master = masters.get("apt_almacen", pd.DataFrame()).copy()
    if apt_master.empty:
        st.error("El maestro apt_almacen viene vacío. Revisa data/Apartamentos e Inventarios.xlsx.")
        st.stop()

    if "Localizacion" not in apt_master.columns and "Localización" in apt_master.columns:
        apt_master = apt_master.rename(columns={"Localización": "Localizacion"})

    has_loc = "Localizacion" in apt_master.columns

    need_cols = {"APARTAMENTO", "ALMACEN"}
    if not need_cols.issubset(set(apt_master.columns)):
        st.error(f"apt_almacen debe tener columnas {need_cols}. Columnas detectadas: {list(apt_master.columns)}")
        st.stop()

    ap_map = apt_master[["APARTAMENTO", "ALMACEN"] + (["Localizacion"] if has_loc else [])].dropna(subset=["APARTAMENTO"]).drop_duplicates()
    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()

    # ✅ FIX: coords robustas (NO split)
    if has_loc:
        ap_map["LAT"], ap_map["LNG"] = parse_lat_lng(ap_map["Localizacion"])
    else:
        ap_map["LAT"] = pd.NA
        ap_map["LNG"] = pd.NA
        st.warning(
            "No se ha encontrado columna 'Localizacion' en apt_almacen. "
            f"Columnas detectadas: {list(apt_master.columns)}. "
            "La app seguirá, pero sin rutas."
        )

    # Aviso formatos malos (sin romper app)
    if has_loc:
        bad = ap_map[ap_map["LAT"].isna() | ap_map["LNG"].isna()][["APARTAMENTO", "Localizacion"]].dropna().head(12)
        if not bad.empty:
            st.warning("Hay localizaciones que no se pudieron parsear (revisa formato). Ejemplos:")
            st.dataframe(bad, use_container_width=True, height=260)

    # Avantio -> APARTAMENTO
    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()

    # Cruces maestros
    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]], on="APARTAMENTO", how="left")

    # Odoo -> ALMACEN
    odoo_norm = odoo_norm.rename(columns={"Ubicación": "ALMACEN"})
    odoo_norm["ALMACEN"] = odoo_norm["ALMACEN"].astype(str).str.strip()

    stock_by_alm = (
        odoo_norm.groupby(["ALMACEN", "Amenity"], as_index=False)["Cantidad"]
        .sum()
        .rename(columns={"Cantidad": "Cantidad"})
    )

    rep = summarize_replenishment(stock_by_alm, masters["thresholds"])
    unclassified = odoo_norm[odoo_norm["Amenity"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        replenishment_df=rep,
        unclassified_products=unclassified,
        period_start=period_start,
        period_days=period_days,
    )

    # KPIs
    kpis = dash.get("kpis", {})
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Entradas (día foco)", kpis.get("entradas_dia", 0))
    c2.metric("Salidas (día foco)", kpis.get("salidas_dia", 0))
    c3.metric("Turnovers", kpis.get("turnovers_dia", 0))
    c4.metric("Ocupados", kpis.get("ocupados_dia", 0))
    c5.metric("Vacíos", kpis.get("vacios_dia", 0))

    st.download_button(
        "⬇️ Descargar Excel (Operativa)",
        data=dash["excel_all"],
        file_name=dash["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    operativa = dash["operativa"].copy()

    # Rutas HOY + MAÑANA
    st.divider()
    st.subheader("📍 Ruta Google Maps · Reposición HOY + MAÑANA (por ZONA)")

    if not has_loc:
        st.info("Sin rutas: falta columna 'Localizacion' en maestro apt_almacen.")
    else:
        tomorrow = (pd.Timestamp(today) + pd.Timedelta(days=1)).date()

        short_df = operativa.copy()
        if "Lista_reponer" in short_df.columns:
            short_df = short_df[short_df["Lista_reponer"].astype(str).str.strip().ne("")].copy()

        short_df = short_df[short_df["Día"].isin([today, tomorrow])].copy()
        short_df = short_df.merge(ap_map[["APARTAMENTO", "LAT", "LNG"]], on="APARTAMENTO", how="left")
        short_df["COORD"] = short_df.apply(
            lambda r: _coord_str(r["LAT"], r["LNG"]) if pd.notna(r.get("LAT")) and pd.notna(r.get("LNG")) else None,
            axis=1
        )

        if short_df.empty:
            st.info("No hay reposiciones previstas para HOY y MAÑANA.")
        else:
            MAX_STOPS = 20
            for dia, ddf in short_df.groupby("Día", dropna=False):
                st.markdown(f"### {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
                for zona, zdf in ddf.groupby("ZONA", dropna=False):
                    zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
                    coords = [c for c in zdf["COORD"].tolist() if c]
                    if not coords:
                        st.info(f"{zona_label}: sin coordenadas suficientes para generar ruta.")
                        continue

                    for idx, chunk in enumerate(chunk_list(coords, MAX_STOPS), start=1):
                        url = build_gmaps_directions_url(chunk, travelmode=travelmode, return_to_base=return_to_base)
                        if url:
                            st.link_button(f"{zona_label} · Ruta (tramo {idx})", url)

    # Tabla operativa
    st.divider()
    st.subheader("PARTE OPERATIVO · Entradas/Salidas/Ocupación/Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Agrupado por ZONA")

    if only_replenishment and "Lista_reponer" in operativa.columns:
        operativa = operativa[operativa["Lista_reponer"].astype(str).str.strip().ne("")].copy()

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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error("⚠️ Error en la app (detalle visible)")
        st.exception(e)
