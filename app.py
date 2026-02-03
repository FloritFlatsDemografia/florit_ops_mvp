import streamlit as st
import pandas as pd


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
- Apt↔Almacén
- Café por apartamento
- Stock mínimo/máximo (thresholds)

✅ Resultado: un **parte operativo por día** con:
- Entradas / Salidas / Ocupados / Vacíos (por apartamento)
- Reposición (Lista_reponer)
- Próxima entrada futura
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

    st.divider()

    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

    operativa = dash["operativa"].copy()

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
