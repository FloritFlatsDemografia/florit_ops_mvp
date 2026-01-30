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

    st.sidebar.header("Parámetros")
    ref_date = st.sidebar.date_input("Fecha de referencia", value=date.today())
    window_days = st.sidebar.slider("Ventana próximos días", min_value=1, max_value=14, value=6)

    masters = load_masters_repo()
    st.sidebar.success("Maestros cargados desde GitHub ✅")

    if not (avantio_file and odoo_file):
        st.info("Sube Avantio + Odoo para generar el dashboard.")
        st.stop()

    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)

    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant (archivo vacío o columnas no detectadas).")
        st.stop()

    odoo_norm = normalize_products(odoo_df)

    ap_map = masters["apt_almacen"][["APARTAMENTO", "ALMACEN"]].dropna().drop_duplicates()
    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()

    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()

    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(ap_map, on="APARTAMENTO", how="left")

    odoo_norm = odoo_norm.rename(columns={"Ubicación": "ALMACEN"})
    odoo_norm["ALMACEN"] = odoo_norm["ALMACEN"].astype(str).str.strip()

    stock_by_alm = (
        odoo_norm.groupby(["ALMACEN", "Amenity"], as_index=False)["Cantidad"]
        .sum()
        .rename(columns={"Cantidad": "Cantidad"})
    )

    rep = summarize_replenishment(stock_by_alm, masters["thresholds"])

    # (se sigue calculando por si más adelante lo reintroduces, pero NO se muestra)
    unclassified = odoo_norm[odoo_norm["Amenity"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        replenishment_df=rep,
        ref_date=ref_date,
        window_days=window_days,
        unclassified_products=unclassified,
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Entradas hoy", int(dash["kpis"]["entradas_hoy"]))
    c2.metric("Salidas hoy", int(dash["kpis"]["salidas_hoy"]))
    c3.metric("Aptos con faltantes (min)", int(dash["kpis"]["aptos_con_faltantes"]))

    st.divider()

    st.subheader("0) PICKING HOY – Todo lo que hay que reponer")
    st.dataframe(dash["picking_hoy"], use_container_width=True, height=360)

    st.download_button(
        "⬇️ Descargar Excel (Picking + dashboards)",
        data=dash["excel_all"],
        file_name=f"FloritOPS_{ref_date.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()

    st.subheader("1) PRIMER PLANO – Entradas HOY (prioridad)")
    st.dataframe(dash["entradas_hoy"], use_container_width=True, height=320)

    st.divider()

    st.subheader("2) ENTRADAS PRÓXIMAS – desde mañana (según ventana)")
    st.dataframe(dash["entradas_proximas"], use_container_width=True, height=320)

    st.divider()

    st.subheader("3) OCUPADOS con salida próxima – según ventana")
    st.dataframe(dash["ocupados_salida_proxima"], use_container_width=True, height=320)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.set_page_config(page_title="Florit OPS – Error", layout="wide")
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
