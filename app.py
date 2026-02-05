import streamlit as st


def main():
    from src.loaders import load_masters_repo
    from src.parsers import parse_avantio_entradas, parse_odoo_stock
    from src.normalize import normalize_products, summarize_replenishment
    from src.dashboard import build_dashboard_frames

    st.set_page_config(page_title="Florit OPS – Operativa & Reposición", layout="wide")
    st.title("Florit OPS – Operativa diaria + reposición (amenities)")

    with st.expander("📌 Cómo usar", expanded=False):
        st.markdown(
            """
**Sube 2 archivos diarios:**
- **Avantio (Entradas)**: .xls / .xlsx / .csv / (xls HTML de Avantio)
- **Odoo (stock.quant)**: .xlsx / .csv

📌 Los **maestros fijos** se cargan automáticamente desde `data/` en GitHub:
- Zonas
- Apt↔Almacén
- Café por apartamento
- Stock mínimo/máximo (thresholds)
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

    masters = load_masters_repo()
    st.sidebar.success("Maestros cargados desde GitHub ✅")

    if not (avantio_file and odoo_file):
        st.info("Sube Avantio + Odoo para generar el dashboard.")
        st.stop()

    # ---------- Parse ----------
    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)

    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant (archivo vacío o columnas no detectadas).")
        st.stop()

    # ---------- Normaliza Odoo ----------
    odoo_norm = normalize_products(odoo_df)

    # ---------- Mapa apt -> almacén ----------
    ap_map = masters["apt_almacen"][["APARTAMENTO", "ALMACEN"]].dropna().drop_duplicates()
    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()

    # Avantio -> APARTAMENTO
    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()

    # Cruces maestros
    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(ap_map, on="APARTAMENTO", how="left")

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

    # Productos sin clasificar (no se muestran)
    unclassified = odoo_norm[odoo_norm["Amenity"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    # ---------- Dashboard ----------
    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        replenishment_df=rep,
        unclassified_products=unclassified,
    )

    # ---------- KPIs (robustos) ----------
    kpis = dash.get("kpis", {})

    c1, c2, c3 = st.columns(3)

    entradas_hoy = kpis.get("entradas_hoy", 0)
    entradas_7d = kpis.get("entradas_proximas_7d", 0)
    libres_3d_kpi = kpis.get("libres_reposicion_3d", None)

    c1.metric("Entradas hoy", int(entradas_hoy) if entradas_hoy is not None else 0)
    c2.metric("Entradas próximas (7 días)", int(entradas_7d) if entradas_7d is not None else 0)

    if libres_3d_kpi is None:
        c3.metric("Libres para reposición (3 días)", "—")
        st.warning("KPI 'libres_reposicion_3d' no disponible (faltan datos o no se calculó).")
    else:
        c3.metric("Libres para reposición (3 días)", int(libres_3d_kpi))

    st.download_button(
        "⬇️ Descargar Excel (Dashboards)",
        data=dash["excel_all"],
        file_name=dash["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()

    st.subheader("1) PRIMER PLANO – Entradas HOY (prioridad)")
    st.dataframe(dash["entradas_hoy"], use_container_width=True, height=340)

    st.divider()

    st.subheader("2) ENTRADAS PRÓXIMAS – 7 días (desde mañana)")
    st.dataframe(dash["entradas_proximas"], use_container_width=True, height=340)

    st.divider()

    st.subheader("3) LIBRES para reposición – 3 días (desde mañana) · agrupado por ZONA")

    # OJO: aquí también debe ser robusto si la clave no existe
    libres = dash.get("libres_reposicion_3d", None)
    if libres is None:
        st.info("No disponible: 'libres_reposicion_3d' no viene en el dashboard (revisar build_dashboard_frames).")
        st.stop()

    libres = libres.copy()
    if libres.empty:
        st.info("No hay apartamentos libres en la ventana de 3 días (desde mañana) con algo que reponer.")
    else:
        # Mostrar por secciones de zona (agrupado visual)
        for zona, zdf in libres.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
            st.markdown(f"### {zona_label}")
            st.dataframe(
                zdf.drop(columns=["ZONA"]),
                use_container_width=True,
                height=min(360, 40 + 35 * len(zdf)),
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
