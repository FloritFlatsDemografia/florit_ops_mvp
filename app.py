import streamlit as st
import pandas as pd
from zoneinfo import ZoneInfo
from urllib.parse import quote
import re


ORIGIN_LAT = 39.45702028460933
ORIGIN_LNG = -0.38498336081567713


# =========================
# Google Maps helpers
# =========================
def _coord_str(lat, lng):
    try:
        return f"{float(lat):.8f},{float(lng):.8f}"
    except Exception:
        return None


def build_gmaps_directions_url(coords, travelmode="walking", return_to_base=False, optimize=True):
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
    if wp and optimize:
        wp = "optimize:true|" + wp

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
    """
    "Detergente x3, Insecticida x1" -> [("Detergente",3),("Insecticida",1)]
    """
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
    """
    Devuelve:
      items_df: Día, ZONA, APARTAMENTO, Producto, Cantidad, Fuente
      totals_df: Producto, Total
    Si include_completar=True, suma Lista_reponer + Completar con.
    """
    df = operativa.copy()

    # Solo estados “preparables”
    df = df[df["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA", "VACIO"])].copy()

    # Zonas filtradas
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

📌 Maestros en `data/` (GitHub):
- Zonas
- Apartamentos e Inventarios (ALMACEN + Localización)
- Café por apartamento
- Stock mínimo/máximo

✅ Flujo:
1) Parte Operativo (por día y ZONA)
2) Sugerencia de Reposición (totales + dónde dejar)
3) Ruta Google Maps (botones), al final
"""
        )

    # =========================
    # Sidebar
    # =========================
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
        only_replenishment = st.checkbox("Mostrar SOLO apartamentos con reposición", value=True)

        estados_sel = st.multiselect(
            "Filtrar estados",
            ["ENTRADA", "SALIDA", "ENTRADA+SALIDA", "OCUPADO", "VACIO"],
            default=["ENTRADA", "SALIDA", "ENTRADA+SALIDA", "OCUPADO", "VACIO"],
        )

        st.divider()
        st.subheader("Ruta (HOY + MAÑANA)")
        travelmode = st.selectbox("Modo", ["walking", "driving"], index=0)
        return_to_base = st.checkbox("Volver a Florit Flats al final", value=False)

    # =========================
    # Load masters
    # =========================
    try:
        masters = load_masters_repo()
        st.sidebar.success("Maestros cargados ✅")
    except Exception as e:
        st.error("Fallo cargando maestros (data/).")
        st.exception(e)
        st.stop()

    # Zonas selector (multi)
    zonas_all = (
        masters["zonas"]["ZONA"].dropna().astype(str).str.strip().unique().tolist()
        if "zonas" in masters and "ZONA" in masters["zonas"].columns
        else []
    )
    zonas_all = sorted([z for z in zonas_all if z and z.lower() not in ["nan", "none"]])

    zonas_sel = st.sidebar.multiselect(
        "ZONAS (multiselección)",
        options=zonas_all,
        default=zonas_all,
    )

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
    # Cruces maestros
    # =========================
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

    ap_map = ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]].dropna(subset=["APARTAMENTO", "ALMACEN"]).drop_duplicates()
    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()

    avantio_df = avantio_df.merge(ap_map, on="APARTAMENTO", how="left")

    # =========================
    # Normaliza Odoo + stock por almacén
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

    # 1) reposición completa (hasta máximo) -> para "Completar con"
    rep_all = summarize_replenishment(
        stock_by_alm,
        masters["thresholds"],
        objective="max",
        urgent_only=False,
    )

    # 2) reposición usada en pantalla (si urgente_only, solo bajo mínimo)
    rep = summarize_replenishment(
        stock_by_alm,
        masters["thresholds"],
        objective="max",
        urgent_only=urgent_only,
    )

    unclassified = odoo_norm[odoo_norm["AmenityKey"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    # =========================
    # Dashboard  (IMPORTANTE: rep_all_df + urgent_only)
    # =========================
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

    # =========================
    # 1) PARTE OPERATIVO
    # =========================
    st.divider()
    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

    operativa = dash["operativa"].copy()

    if zonas_sel:
        operativa = operativa[operativa["ZONA"].isin(zonas_sel)].copy()

    if estados_sel:
        operativa = operativa[operativa["Estado"].isin(estados_sel)].copy()

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

    # =========================
    # 2) SUGERENCIA DE REPOSICIÓN
    # =========================
    st.divider()
    st.subheader("Sugerencia de Reposición")

    if urgent_only:
        st.caption("Modo URGENTE: Totales + dónde dejar, incluyendo Lista_reponer (urgente) y Completar con (aprovechar viaje).")
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
            st.dataframe(
                items_df,
                use_container_width=True,
                height=min(520, 40 + 28 * min(len(items_df), 25)),
            )

    # =========================
    # 3) RUTAS GOOGLE MAPS (ABAJO)
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

    # en urgente, si quieres rutear solo urgentes -> Lista_reponer; si quieres incluir completar, cambia aquí
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
                    url = build_gmaps_directions_url(chunk, travelmode=travelmode, return_to_base=return_to_base, optimize=True)
                    if url:
                        st.link_button(f"Abrir ruta · {zona_label} (tramo {idx})", url)

    # =========================
    # Debug opcional
    # =========================
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
