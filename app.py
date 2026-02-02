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
- **Avantio (Entradas)**: .xls / .xlsx / .csv
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

    # ---------- Mapea apt -> almacén ----------
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

    # No lo mostramos, pero se lo pasamos por compatibilidad (si lo vuelves a activar)
    unclassified = odoo_norm[odoo_norm["Amenity"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    # ---------- Dashboard ----------
    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        replenishment_df=rep,
        unclassified_products=unclassified,
    )

    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Entradas hoy", int(dash["kpis"]["entradas_hoy"]))
    c2.metric("Entradas próximas (7 días)", int(dash["kpis"]["entradas_proximas_7d"]))
    c3.metric("Ocupados con salida próxima (7 días)", int(dash["kpis"]["ocupados_salida_prox_7d"]))

    st.download_button(
        "⬇️ Descargar Excel (Dashboards)",
        data=dash["excel_all"],
        file_name=dash["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()

    st.subheader("1) PRIMER PLANO – Entradas HOY")
    st.dataframe(dash["entradas_hoy"], use_container_width=True, height=340)

    st.divider()

    st.subheader("2) ENTRADAS PRÓXIMAS – 7 días (desde mañana)")
    st.dataframe(dash["entradas_proximas"], use_container_width=True, height=340)

    st.divider()

    st.subheader("3) OCUPADOS con salida próxima – 7 días (desde mañana)")
    st.dataframe(dash["ocupados_salida_proxima"], use_container_width=True, height=340)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.set_page_config(page_title="Florit OPS – Error", layout="wide")
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
