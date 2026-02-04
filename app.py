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


def _parse_lat_lng_from_localizacion(series: pd.Series):
    """
    Acepta formatos como:
      "39.49,-0.39"
      "39.49, -0.39"
      "(39.49, -0.39)"
      "39.49; -0.39"
    """
    s = series.astype(str).str.strip()
    s = s.str.replace("(", "", regex=False).str.replace(")", "", regex=False)
    # extrae dos floats separados por coma/; o espacios
    ext = s.str.extract(r"([+-]?\d+(?:\.\d+)?)\s*[,; ]\s*([+-]?\d+(?:\.\d+)?)")
    lat = pd.to_numeric(ext[0], errors="coerce")
    lng = pd.to_numeric(ext[1], errors="coerce")
    return lat, lng


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
**2 clics:**
1) Sube **Avantio (Entradas)**
2) Sube **Odoo (stock.quant)**

📌 Los maestros se cargan desde `data/` (GitHub):
- Zonas
- Apt↔Almacén (incluye `Localizacion`)
- Café por apartamento
- Stock mínimo/máximo (thresholds)

✅ La ruta se genera **por apartamento**, seleccionando:
- **Con reposición**
- **Libres HOY y MAÑANA**
- **Con coordenadas**
"""
        )

    # =========================
    # Sidebar
    # =========================
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

    st.sidebar.divider()
    with st.sidebar.expander("Avanzado (opcional)", expanded=False):
        period_start = st.date_input("Inicio", value=today)
        period_days = st.number_input("Nº días", min_value=1, max_value=14, value=2, step=1)
        only_replenishment = st.checkbox("Mostrar SOLO apartamentos con reposición", value=True)

        st.markdown("---")
        st.markdown("**Ruta (reposiciones HOY + MAÑANA)**")
        travelmode = st.selectbox("Modo", ["walking", "driving"], index=0)
        return_to_base = st.checkbox("Volver a Florit Flats al final", value=False)

    # =========================
    # Maestros
    # =========================
    try:
        masters = load_masters_repo()
        st.sidebar.success("Maestros cargados ✅")
    except Exception as e:
        st.error("Fallo cargando maestros (data/).")
        st.exception(e)
        st.stop()

    if not (avantio_file and odoo_file):
        st.info("Sube Avantio + Odoo para generar el parte operativo.")
        st.stop()

    # =========================
    # Parse
    # =========================
    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)

    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant (archivo vacío o columnas no detectadas).")
        st.stop()

    # =========================
    # Normaliza Odoo
    # =========================
    odoo_norm = normalize_products(odoo_df)

    # =========================
    # Maestro apt_almacen + coordenadas
    # =========================
    apt_master = masters["apt_almacen"].copy()

    # acepta Localización con acento
    if "Localizacion" not in apt_master.columns and "Localización" in apt_master.columns:
        apt_master = apt_master.rename(columns={"Localización": "Localizacion"})

    if "Localizacion" not in apt_master.columns:
        st.warning("No se ha encontrado columna 'Localizacion' en el maestro apt_almacen. La app funcionará pero sin rutas.")
        apt_master["Localizacion"] = pd.NA

    ap_map = apt_master[["APARTAMENTO", "ALMACEN", "Localizacion"]].copy()
    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()

    ap_map["LAT"], ap_map["LNG"] = _parse_lat_lng_from_localizacion(ap_map["Localizacion"])

    # =========================
    # Avantio -> APARTAMENTO y cruces maestros
    # =========================
    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()

    # Cruces maestros
    masters["zonas"]["APARTAMENTO"] = masters["zonas"]["APARTAMENTO"].astype(str).str.strip()
    masters["cafe"]["APARTAMENTO"] = masters["cafe"]["APARTAMENTO"].astype(str).str.strip()

    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]], on="APARTAMENTO", how="left")

    # =========================
    # Odoo -> ALMACEN (desde Ubicación)
    # =========================
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

    # Unclassified (por si luego lo quieres mostrar)
    unclassified = odoo_norm[odoo_norm["Amenity"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    # =========================
    # Dashboard (operativa)
    # =========================
    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        replenishment_df=rep,
        unclassified_products=unclassified,
        period_start=period_start,
        period_days=period_days,
    )

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

    st.download_button(
        "⬇️ Descargar Excel (Operativa)",
        data=dash["excel_all"],
        file_name=dash["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    operativa = dash["operativa"].copy()

       # =========================
    # RUTA: por APARTAMENTO (HOY y MAÑANA), "visitable" + con reposición
    # =========================
    st.divider()
    st.subheader("📍 Ruta Google Maps · Reposición HOY + MAÑANA (por ZONA)")
    st.caption("Incluye apartamentos con reposición y Estado visitable ese día. Salida: Florit Flats.")

    tomorrow = (pd.Timestamp(today) + pd.Timedelta(days=1)).date()

    # Asegura tipo date en la columna Día
    operativa_route = operativa.copy()
    operativa_route["Día"] = pd.to_datetime(operativa_route["Día"], errors="coerce").dt.date

    # Estados visitables (ajusta aquí si quieres restringir)
    VISITABLE_STATES = {"VACIO", "ENTRADA", "SALIDA", "ENTRADA+SALIDA"}

    def build_routes_for_day(day_date):
        df = operativa_route.copy()

        # 1) Solo ese día
        df = df[df["Día"] == day_date].copy()

        # 2) Solo con reposición
        if "Lista_reponer" in df.columns:
            df = df[df["Lista_reponer"].astype(str).str.strip().ne("")].copy()
        else:
            return pd.DataFrame()

        # 3) Solo estados visitables
        df = df[df["Estado"].astype(str).isin(VISITABLE_STATES)].copy()

        # 4) Añadir coords por apartamento (por si operativa no las lleva)
        df = df.merge(ap_map[["APARTAMENTO", "LAT", "LNG"]], on="APARTAMENTO", how="left")
        df["COORD"] = df.apply(
            lambda r: _coord_str(r["LAT"], r["LNG"]) if pd.notna(r.get("LAT")) and pd.notna(r.get("LNG")) else None,
            axis=1,
        )
        df = df[df["COORD"].notna()].copy()

        # 5) Una parada por apartamento
        df = df.drop_duplicates("APARTAMENTO").copy()

        return df

    for day_date, day_label in [(today, "HOY"), (tomorrow, "MAÑANA")]:
        st.markdown(f"### {day_label} · {pd.to_datetime(day_date).strftime('%d/%m/%Y')}")

        day_df = build_routes_for_day(day_date)

        if day_df.empty:
            st.info(f"No hay apartamentos visitables con reposición para {day_label} (o faltan coordenadas).")
            continue

        # Listado control
        st.caption("Paradas incluidas (control):")
        st.dataframe(
            day_df.sort_values(["ZONA", "APARTAMENTO"])[
                ["ZONA", "APARTAMENTO", "Estado", "Próxima Entrada", "Lista_reponer"]
            ],
            use_container_width=True,
            height=260,
        )

        MAX_STOPS = 20
        for zona, zdf in day_df.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"

            coords = zdf["COORD"].tolist()
            if not coords:
                st.info(f"{zona_label}: sin coordenadas suficientes para generar ruta.")
                continue

            for idx, chunk in enumerate(chunk_list(coords, MAX_STOPS), start=1):
                url = build_gmaps_directions_url(chunk, travelmode=travelmode, return_to_base=return_to_base)
                if url:
                    st.link_button(f"{zona_label} · Ruta (tramo {idx})", url)


    # =========================
    # Tabla operativa
    # =========================
    st.divider()
    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

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
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
