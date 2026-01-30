import streamlit as st
from datetime import date


def main():
    from src.loaders import load_masters_repo
    from src.parsers import parse_avantio_entradas, parse_odoo_stock
    from src.normalize import normalize_products, summarize_replenishment
    from src.dashboard import build_dashboard_frames

    st.set_page_config(page_title="Florit OPS – Operativa & Reposición", layout="wide")
    st.title("Florit OPS – Operativa diaria + reposición (amenities)")

    with st.expander("📌 Cómo usar", expanded=False):
        st.markdown("""
**Sube 2 archivos diarios:**
- **Avantio (Entradas)**
- **Odoo (stock.quant)**

Los **maestros fijos** (Zonas, Apt↔Almacén, Café, Min/Max) se cargan automáticamente desde `data/` en GitHub.
""")

    # Sidebar uploads diarios
    st.sidebar.header("Archivos diarios")
    avantio_file = st.sidebar.file_uploader("Avantio (Entradas) .xls/.xlsx/.csv", type=["xls", "xlsx", "csv", "html"])
    odoo_file = st.sidebar.file_uploader("Odoo (stock.quant) .xlsx/.csv", type=["xlsx", "csv"])

    st.sidebar.header("Parámetros")
    ref_date = st.sidebar.date_input("Fecha de referencia", value=date.today())
    window_days = st.sidebar.slider("Ventana próximos días", min_value=1, max_value=14, value=5)

    # Maestros fijos desde repo/data
    masters = load_masters_repo()
    st.sidebar.success("Maestros cargados desde GitHub ✅")

    if not (avantio_file and odoo_file):
        st.info("Sube Avantio + Odoo para generar el dashboard.")
        st.stop()

    # Parse diarios
    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)

    # Normalización productos Odoo → Amenity
    odoo_norm = normalize_products(odoo_df)

    # Map Apt → Almacén
    ap_map = masters["apt_almacen"][["APARTAMENTO", "ALMACEN"]].dropna().drop_duplicates()
    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()

    # Avantio → APARTAMENTO (nombre) + cruces maestros
    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()
    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(ap_map, on="APARTAMENTO", how="left")

    # Stock por almacén
    odoo_norm = odoo_norm.rename(columns={"Ubicación": "ALMACEN"})
    stock_by_alm = odoo_norm.groupby(["ALMACEN", "Amenity"], as_index=False)["Cantidad"].sum()

    # Reposición min/max
    rep = summarize_replenishment(stock_by_alm, masters["thresholds"])

    # Productos no clasificados
    unclassified = odoo_norm[odoo_norm["Amenity"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        replenishment_df=rep,
        ref_date=ref_date,
        window_days=window_days,
        unclassified_products=unclassified
    )

    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Entradas hoy", int(dash["kpis"]["entradas_hoy"]))
    c2.metric("Salidas hoy", int(dash["kpis"]["salidas_hoy"]))
    c3.metric("Aptos con faltantes (min)", int(dash["kpis"]["aptos_con_faltantes"]))

    st.divider()

    # ✅ BLOQUE 0: PICKING HOY (lo que tú necesitas SIEMPRE)
    st.subheader("0) PICKING HOY – Todo lo que hay que reponer")
    st.dataframe(dash["picking_hoy"], use_container_width=True, height=360)

    st.download_button(
        "⬇️ Descargar Excel (Picking + dashboards)",
        data=dash["excel_all"],
        file_name=f"FloritOPS_{ref_date.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()

    # BLOQUE 1: Entradas HOY (si hoy hay 0, saldrá vacío y está bien)
    st.subheader("1) PRIMER PLANO – Entradas HOY (prioridad)")
    st.dataframe(dash["entradas_hoy"], use_container_width=True, height=320)

    st.divider()

    # BLOQUE 2
    st.subheader("2) ENTRADAS PRÓXIMAS – según ventana")
    st.dataframe(dash["entradas_proximas"], use_container_width=True, height=320)

    st.divider()

    # BLOQUE 3
    st.subheader("3) OCUPADOS con salida próxima – según ventana")
    st.dataframe(dash["ocupados_salida_proxima"], use_container_width=True, height=320)

    st.divider()

    # QC
    st.subheader("Control de calidad")
    a, b = st.columns(2)
    with a:
        st.markdown("**Apartamentos sin zona:**")
        st.dataframe(dash["qc_no_zona"], use_container_width=True, height=220)
    with b:
        st.markdown("**Apartamentos sin almacén:**")
        st.dataframe(dash["qc_no_almacen"], use_container_width=True, height=220)

    st.markdown("**Productos Odoo sin clasificar:**")
    st.dataframe(dash["qc_unclassified_products"], use_container_width=True, height=260)


try:
    main()
except Exception as e:
    st.set_page_config(page_title="Florit OPS – Error", layout="wide")
    st.title("⚠️ Error en la app (detalle visible)")
    st.exception(e)
