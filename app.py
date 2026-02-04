import streamlit as st
import pandas as pd
from zoneinfo import ZoneInfo
from urllib.parse import quote
import re


# =========================
# Config ruta (Google Maps)
# =========================
ORIGIN_LAT = 39.45702028460933
ORIGIN_LNG = -0.38498336081567713


COORD_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)")


def _coord_from_text(x: str):
    if not isinstance(x, str):
        return (None, None)
    m = COORD_RE.search(x.strip())
    if not m:
        return (None, None)
    try:
        return (float(m.group(1)), float(m.group(2)))
    except Exception:
        return (None, None)


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
1) Sube Avantio + Odoo
2) Mira el parte (y abre la ruta)

📌 Maestros desde `data/` (GitHub):
- Zonas
- Apt↔Almacén (incluye Localizacion)
- Café por apartamento
- Stock mínimo/máximo (thresholds)

📍 Ruta: se genera con apartamentos **visitable HOY/MAÑANA** y con **Lista_reponer**.
Visitable = **VACIO / ENTRADA / SALIDA / ENTRADA+SALIDA**.
"""
        )

    # ---- Uploads (2 clics) ----
    st.sidebar.header("Archivos diarios")
    avantio_file = st.sidebar.file_uploader("Avantio (Entradas) .xls/.xlsx/.csv", type=["xls", "xlsx", "csv", "html"])
    odoo_file = st.sidebar.file_uploader("Odoo (stock.quant) .xlsx/.csv", type=["xlsx", "csv"])

    # ---- Avanzado (opcional) ----
    with st.sidebar.expander("Avanzado (opcional)", expanded=False):
        period_start = st.date_input("Inicio", value=pd.Timestamp.today().date())
        period_days = st.number_input("Nº días", min_value=1, max_value=14, value=2, step=1)
        only_replenishment = st.checkbox("Mostrar SOLO apartamentos con reposición", value=True)
        travelmode = st.selectbox("Ruta: modo", ["walking", "driving"], index=0)
        return_to_base = st.checkbox("Ruta: volver a Florit Flats", value=False)

    # Defaults si el expander está cerrado (Streamlit igualmente mantiene values, pero por claridad)
    if "period_start" not in locals():
        period_start = pd.Timestamp.today().date()
    if "period_days" not in locals():
        period_days = 2
    if "only_replenishment" not in locals():
        only_replenishment = True
    if "travelmode" not in locals():
        travelmode = "walking"
    if "return_to_base" not in locals():
        return_to_base = False

    # ---- Maestros ----
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

    # ---------- Parse ----------
    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)

    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant (archivo vacío o columnas no detectadas).")
        st.stop()

    # ---------- Normaliza Odoo ----------
    odoo_norm = normalize_products(odoo_df)

    # ---------- Maestro apt_almacen + coords ----------
    apt_master = masters["apt_almacen"].copy()
    # soporte nombres raros si existieran
    if "Localizacion" not in apt_master.columns and "Localización" in apt_master.columns:
        apt_master = apt_master.rename(columns={"Localización": "Localizacion"})
    if "Localizacion" not in apt_master.columns and "Localiación" in apt_master.columns:
        apt_master = apt_master.rename(columns={"Localiación": "Localizacion"})

    if "Localizacion" not in apt_master.columns:
        apt_master["Localizacion"] = ""

    apt_master["APARTAMENTO"] = apt_master["APARTAMENTO"].astype(str).str.strip()
    apt_master["ALMACEN"] = apt_master["ALMACEN"].astype(str).str.strip()

    latlng = apt_master["Localizacion"].apply(lambda x: _coord_from_text(str(x)))
    apt_master["LAT"] = latlng.apply(lambda t: t[0])
    apt_master["LNG"] = latlng.apply(lambda t: t[1])

    # Avantio -> APARTAMENTO
    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()

    # Cruces maestros
    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(apt_master[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]], on="APARTAMENTO", how="left")

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

    # -------- Avisos coords --------
    missing_coords = apt_master[apt_master["LAT"].isna() | apt_master["LNG"].isna()]["APARTAMENTO"].dropna().unique().tolist()
    if missing_coords:
        st.warning(f"Faltan coordenadas parseables en {len(missing_coords)} apartamentos (no entrarán en rutas). Ej: {', '.join(missing_coords[:8])}")

    # ==========================
    # RUTAS HOY + MAÑANA (criterio correcto)
    # ==========================
    st.divider()
    st.subheader("📍 Ruta Google Maps · Reposición HOY + MAÑANA (por ZONA)")
    st.caption("Criterio: Estado visitable (VACIO/ENTRADA/SALIDA/ENTRADA+SALIDA) + Lista_reponer + coordenadas. Salida: Florit Flats.")

    operativa = dash["operativa"].copy()

    # Normaliza Día a date (clave para que el filtro no falle)
    operativa["Día"] = pd.to_datetime(operativa["Día"], errors="coerce").dt.date

    # Normaliza Estado
    operativa["Estado"] = operativa["Estado"].astype(str).str.strip().str.upper()

    # Merge coords por apartamento (por si el dashboard no los arrastra)
    operativa = operativa.merge(apt_master[["APARTAMENTO", "LAT", "LNG"]], on="APARTAMENTO", how="left")

    operativa["COORD"] = operativa.apply(
        lambda r: f"{float(r['LAT']):.8f},{float(r['LNG']):.8f}" if pd.notna(r.get("LAT")) and pd.notna(r.get("LNG")) else None,
        axis=1,
    )

    # HOY / MAÑANA relativos al periodo elegido (2 clics coherente)
    d0 = pd.to_datetime(period_start).date()
    d1 = (pd.Timestamp(d0) + pd.Timedelta(days=1)).date()

    visitable_states = {"VACIO", "ENTRADA", "SALIDA", "ENTRADA+SALIDA"}

    def _build_routes_for_day(day: pd.Timestamp.date, label: str):
        st.markdown(f"### {label} · {pd.to_datetime(day).strftime('%d/%m/%Y')}")

        df_day = operativa[operativa["Día"] == day].copy()

        # solo visitables
        df_day = df_day[df_day["Estado"].isin(visitable_states)].copy()

        # solo con reposición
        df_day = df_day[df_day["Lista_reponer"].astype(str).str.strip().ne("")].copy()

        # solo con coords
        df_day = df_day[df_day["COORD"].notna()].copy()

        if df_day.empty:
            st.info(f"No hay apartamentos visitables con reposición para {label} (o faltan coordenadas).")
            return

        MAX_STOPS = 20  # waypoints por tramo

        # Por ZONA (como querías), pero ya filtrado por apartamento
        for zona, zdf in df_day.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"

            # Dedup por apartamento (por si hubiese duplicados)
            zdf = zdf.drop_duplicates(subset=["APARTAMENTO"]).copy()

            # Orden: primero entradas (si las quieres arriba), luego vacíos, etc.
            prio = {"ENTRADA+SALIDA": 0, "ENTRADA": 1, "SALIDA": 2, "VACIO": 3, "OCUPADO": 9}
            zdf["__rprio"] = zdf["Estado"].map(prio).fillna(99).astype(int)
            zdf = zdf.sort_values(["__rprio", "APARTAMENTO"])

            coords = zdf["COORD"].tolist()

            st.markdown(f"#### {zona_label} · {len(coords)} paradas")

            # Mostrar mini tabla de paradas
            show_cols = ["APARTAMENTO", "Estado", "Próxima Entrada", "Lista_reponer"]
            show_cols = [c for c in show_cols if c in zdf.columns]
            st.dataframe(zdf[show_cols].reset_index(drop=True), use_container_width=True, height=min(360, 40 + 35 * len(zdf)))

            for idx, chunk in enumerate(chunk_list(coords, MAX_STOPS), start=1):
                url = build_gmaps_directions_url(chunk, travelmode=travelmode, return_to_base=return_to_base)
                if url:
                    st.link_button(f"Abrir ruta {zona_label} (tramo {idx})", url)

    _build_routes_for_day(d0, "HOY")
    _build_routes_for_day(d1, "MAÑANA")

    # ==========================
    # TABLAS (lo de siempre)
    # ==========================
    st.divider()
    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

    if only_replenishment:
        operativa_show = operativa[operativa["Lista_reponer"].astype(str).str.strip().ne("")].copy()
    else:
        operativa_show = operativa.copy()

    # Orden global: Día, ZONA, prioridad, apartamento
    if "__prio" in operativa_show.columns:
        operativa_show = operativa_show.sort_values(["Día", "ZONA", "__prio", "APARTAMENTO"])
    else:
        operativa_show = operativa_show.sort_values(["Día", "ZONA", "APARTAMENTO"])

    for dia, ddf in operativa_show.groupby("Día", dropna=False):
        st.markdown(f"### Día {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
        if ddf.empty:
            st.info("Sin datos.")
            continue

        for zona, zdf in ddf.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
            st.markdown(f"#### {zona_label}")

            drop_cols = ["ZONA"]
            if "__prio" in zdf.columns:
                drop_cols.append("__prio")

            show_df = zdf.drop(columns=drop_cols, errors="ignore").copy()
            st.dataframe(_style_operativa(show_df), use_container_width=True, height=min(520, 40 + 35 * len(show_df)))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
