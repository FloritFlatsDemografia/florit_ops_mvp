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
    # limpiar y deduplicar manteniendo orden
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
# Estilos tabla operativa
# =========================
def _style_operativa(df: pd.DataFrame):
    """
    Colorea filas según Estado.
    """
    colors = {
        "ENTRADA+SALIDA": "#FFF3BF",  # amarillo suave
        "ENTRADA": "#D3F9D8",         # verde suave
        "SALIDA": "#FFE8CC",          # naranja suave
        "OCUPADO": "#E7F5FF",         # azul suave
        "VACIO": "#F1F3F5",           # gris suave
    }

    def row_style(row):
        bg = colors.get(str(row.get("Estado", "")), "")
        if bg:
            return [f"background-color: {bg}"] * len(row)
        return [""] * len(row)

    return df.style.apply(row_style, axis=1)


def main():
    from src.loaders import load_masters_repo
    from src.parsers import parse_avantio_entradas, parse_odoo_stock
    from src.normalize import normalize_products, summarize_replenishment
    from src.dashboard import build_dashboard_frames

    st.set_page_config(page_title="Florit OPS – Operativa & Reposición", layout="wide")
    st.title("Florit OPS – Parte diario (Operativa + Reposición)")

    with st.expander("📌 Cómo usar", expanded=False):
        st.markdown(
            """
**Sube 2 archivos diarios:**
- **Avantio (Entradas)**: .xls / .xlsx / .csv / (xls HTML de Avantio)
- **Odoo (stock.quant)**: .xlsx / .csv

📌 Los **maestros fijos** se cargan automáticamente desde `data/` en GitHub:
- Zonas
- Apt↔Almacén (incluye Localizacion lat,lng)
- Café por apartamento
- Stock mínimo/máximo (thresholds)

✅ Resultado: un **parte operativo por día** con:
- Entradas / Salidas / Ocupados / Vacíos (por apartamento)
- Reposición (Lista_reponer)
- Próxima entrada futura

📍 NUEVO: **Ruta Google Maps** para reposición HOY y MAÑANA, por ZONA (salida: Florit Flats).
"""
        )

    st.sidebar.header("Archivos diarios")
    avantio_file = st.sidebar.file_uploader(
        "Avantio (Entradas) .xls/.xlsx/.csv",
        type=["xls", "xlsx", "csv", "html"],
    )
    odoo_file = st.sidebar.file_uploader(
        "Odoo (stock.quant) .xlsx/.csv",
        type=["xlsx", "csv"],
    )

    st.sidebar.divider()
    st.sidebar.header("Periodo operativo")
    period_start = st.sidebar.date_input("Inicio", value=pd.Timestamp.today().date())
    period_days = st.sidebar.number_input("Nº días", min_value=1, max_value=14, value=2, step=1)

    st.sidebar.divider()
    only_replenishment = st.sidebar.checkbox("Mostrar SOLO apartamentos con reposición", value=True)

    st.sidebar.divider()
    st.sidebar.header("Ruta (reposiciones hoy + mañana)")
    travelmode = st.sidebar.selectbox("Modo", ["walking", "driving"], index=0)
    return_to_base = st.sidebar.checkbox("Volver a Florit Flats al final", value=False)

    masters = load_masters_repo()
    st.sidebar.success("Maestros cargados desde GitHub ✅")

    if not (avantio_file and odoo_file):
        st.info("Sube Avantio + Odoo para generar el parte operativo.")
        st.stop()

    # ---------- Parse ----------
    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)

    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant (archivo vacío o columnas no detectadas).")
        st.stop()

    # ---------- Normaliza Odoo ----------
    odoo_norm = normalize_products(odoo_df)

    # ---------- Mapa apt -> almacén + localización ----------
    # Se asume que el maestro apt_almacen ya tiene columna "Localizacion" con "lat,lng"
    apt_master = masters["apt_almacen"].copy()

    # Intento robusto: si por cualquier motivo la columna viniera con acento
    if "Localizacion" not in apt_master.columns and "Localización" in apt_master.columns:
        apt_master = apt_master.rename(columns={"Localización": "Localizacion"})

    required_cols = {"APARTAMENTO", "ALMACEN", "Localizacion"}
    missing_cols = required_cols - set(apt_master.columns)
    if missing_cols:
        st.error(f"Faltan columnas en maestro apt_almacen: {missing_cols}. Revisa el Excel de GitHub.")
        st.stop()

    ap_map = apt_master[["APARTAMENTO", "ALMACEN", "Localizacion"]].copy()
    ap_map = ap_map.dropna(subset=["APARTAMENTO"]).drop_duplicates()

    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()

    # Parse Localizacion -> LAT/LNG
    loc = ap_map["Localizacion"].astype(str).str.replace(" ", "", regex=False)
    parts = loc.str.split(",", n=1, expand=True)
    ap_map["LAT"] = pd.to_numeric(parts[0], errors="coerce")
    ap_map["LNG"] = pd.to_numeric(parts[1], errors="coerce")

    # Avantio -> APARTAMENTO
    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()

    # Cruces maestros
    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")

    # añade almacén + coords
    avantio_df = avantio_df.merge(ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]], on="APARTAMENTO", how="left")

    # Odoo -> ALMACEN (desde Ubicación)
    odoo_norm = odoo_norm.rename(columns={"Ubicación": "ALMACEN"})
    odoo_norm["ALMACEN"] = odoo_norm["ALMACEN"].astype(str).str.strip()

    # Stock por almacén + amenity
    stock_by_alm = (
        odoo_norm.groupby(["ALMACEN", "Amenity"], as_index=False)["Cantidad"]
        .sum()
        .rename(columns={"Cantidad": "Cantidad"})
    )

    # Reposición min/max
    rep = summarize_replenishment(stock_by_alm, masters["thresholds"])

    # Productos sin clasificar (por si luego quieres mostrarlo)
    unclassified = odoo_norm[odoo_norm["Amenity"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    # ---------- Dashboard ----------
    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        replenishment_df=rep,
        unclassified_products=unclassified,
        period_start=period_start,
        period_days=period_days,
    )

    # ---------- KPIs ----------
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

    # Aviso si faltan coordenadas en el maestro (de cara a rutas)
    missing_coords = ap_map[ap_map["LAT"].isna() | ap_map["LNG"].isna()]["APARTAMENTO"].dropna().unique().tolist()
    if missing_coords:
        st.warning(
            f"Faltan coordenadas en {len(missing_coords)} apartamentos (no entrarán en rutas). "
            f"Ej: {', '.join(missing_coords[:8])}"
        )

    st.divider()

    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

    operativa = dash["operativa"].copy()

    # ============
    # RUTAS: reposición HOY y MAÑANA (corto plazo)
    # ============
    st.divider()
    st.subheader("📍 Ruta Google Maps · Reposición HOY + MAÑANA (por ZONA)")
    st.caption("Incluye solo apartamentos con Lista_reponer y coordenadas. Salida: Florit Flats.")

    tz = ZoneInfo("Europe/Madrid")
    today = pd.Timestamp.now(tz=tz).normalize().date()
    tomorrow = (pd.Timestamp(today) + pd.Timedelta(days=1)).date()

    # Solo hoy y mañana + con reposición
    short_df = operativa.copy()
    if "Lista_reponer" in short_df.columns:
        short_df = short_df[short_df["Lista_reponer"].astype(str).str.strip().ne("")].copy()
    short_df = short_df[short_df["Día"].isin([today, tomorrow])].copy()

    # Añadir coords a operativa (merge por APARTAMENTO desde ap_map)
    short_df = short_df.merge(ap_map[["APARTAMENTO", "LAT", "LNG"]], on="APARTAMENTO", how="left")
    short_df["COORD"] = short_df.apply(
        lambda r: _coord_str(r["LAT"], r["LNG"]) if pd.notna(r.get("LAT")) and pd.notna(r.get("LNG")) else None,
        axis=1
    )

    # Si quieres solo rutas para mañana (por ejemplo), lo cambias aquí.
    if short_df.empty:
        st.info("No hay reposiciones previstas para HOY y MAÑANA en el periodo seleccionado (o no hay Lista_reponer).")
    else:
        MAX_STOPS = 20  # por seguridad (waypoints en Google Maps)
        # Por cada día, por cada zona
        for dia, ddf in short_df.groupby("Día", dropna=False):
            st.markdown(f"### {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
            for zona, zdf in ddf.groupby("ZONA", dropna=False):
                zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
                coords = [c for c in zdf["COORD"].tolist() if c]

                # Si no hay coords suficientes, aviso
                if not coords:
                    st.info(f"{zona_label}: sin coordenadas suficientes para generar ruta.")
                    continue

                # Dividir en tramos si hay muchos puntos
                for idx, chunk in enumerate(chunk_list(coords, MAX_STOPS), start=1):
                    url = build_gmaps_directions_url(chunk, travelmode=travelmode, return_to_base=return_to_base)
                    if url:
                        st.markdown(f"**{zona_label} · Ruta (tramo {idx})**: {url}")

    st.divider()

    # ============
    # Tablas operativas (lo que ya tenías)
    # ============
    # Filtro solo con reposición (opcional)
    if only_replenishment and "Lista_reponer" in operativa.columns:
        operativa = operativa[operativa["Lista_reponer"].astype(str).str.strip().ne("")].copy()

    # Orden global: Día, ZONA, prioridad, apartamento
    operativa = operativa.sort_values(["Día", "ZONA", "__prio", "APARTAMENTO"])

    # Mostrar por día y por zona
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
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
