import streamlit as st
import pandas as pd
from datetime import date, timedelta
from src.loaders import load_masters
from src.parsers import parse_avantio_entradas, parse_odoo_stock
from src.normalize import normalize_products, summarize_replenishment
from src.dashboard import build_dashboard_frames

st.set_page_config(page_title="Florit OPS – Reposición & Operativa", layout="wide")

st.title("Florit OPS – Operativa diaria + reposición (amenities)")

with st.expander("📌 Cómo usar", expanded=False):
    st.markdown("""
**Inputs diarios (2 archivos):**
1. **Avantio**: export tipo *Entradas* (como tu `.xls`)
2. **Odoo**: export `stock.quant` por ubicación/apartamento (tu `.xlsx`)

La app cruza:
- `Alojamiento` (Avantio) → `APARTAMENTO` (maestro) → `ALMACEN`/`Ubicación` (Odoo)
- Productos Odoo → **amenities genéricos** (reglas robustas por patrones)

Luego genera 3 bloques:
1) **PRIMER PLANO**: entradas hoy + faltantes / a reponer (prioridad)
2) **ENTRADAS PRÓXIMAS**: próximos N días en los grupos con entradas hoy
3) **OCUPADOS con salida próxima**: fuera de grupos con entradas hoy
""")

st.sidebar.header("Archivos")
avantio_file = st.sidebar.file_uploader("Avantio (Entradas) .xls/.xlsx/.csv", type=["xls","xlsx","csv","html"])
odoo_file = st.sidebar.file_uploader("Odoo (stock.quant) .xlsx/.csv", type=["xlsx","csv"])

st.sidebar.header("Parámetros")
ref_date = st.sidebar.date_input("Fecha de referencia", value=date.today())
window_days = st.sidebar.slider("Ventana próximos días", min_value=1, max_value=14, value=5)

st.sidebar.header("Maestros")
masters = load_masters()
st.sidebar.success("Maestros cargados desde el repo")

if not avantio_file or not odoo_file:
    st.info("Sube los 2 archivos (Avantio + Odoo) para generar el dashboard.")
    st.stop()

# 1) Parse Avantio + Odoo
avantio_df = parse_avantio_entradas(avantio_file)
odoo_df = parse_odoo_stock(odoo_file)

# 2) Normalización productos (incluye cápsulas de café)
odoo_norm = normalize_products(odoo_df)

# 3) Enriquecimiento: unir maestros
# Map Alojamiento (Avantio) -> APARTAMENTO (maestro). Por defecto asumimos que coincide.
# Si no coincide, se reporta como "no mapeado".
ap_map = masters["apt_almacen"][["APARTAMENTO","ALMACEN"]].dropna().drop_duplicates()
ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()

# Cruce Avantio a APARTAMENTO
avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()

# Unir zona y café
zona_map = masters["zonas"]
cafe_map = masters["cafe"]

avantio_df = avantio_df.merge(zona_map, on="APARTAMENTO", how="left")
avantio_df = avantio_df.merge(cafe_map, on="APARTAMENTO", how="left")

# Unir almacén
avantio_df = avantio_df.merge(ap_map, on="APARTAMENTO", how="left")

# Unir stock por ALMACEN
# (odoo export usa columna "Ubicación"; la normalizamos a "ALMACEN")
odoo_norm = odoo_norm.rename(columns={"Ubicación":"ALMACEN"})
stock_by_alm = odoo_norm.groupby(["ALMACEN","Amenity"], as_index=False)["Cantidad"].sum()

# 4) Reposición según min/max (por ahora global por amenity; editable en src/thresholds.py)
rep = summarize_replenishment(stock_by_alm, masters["thresholds"], masters["cafe_capsule_rules"])

# 5) Dashboard
dash = build_dashboard_frames(
    avantio_df=avantio_df,
    replenishment_df=rep,
    ref_date=ref_date,
    window_days=window_days
)

# ======== UI ========
col1, col2, col3 = st.columns([1,1,1])
col1.metric("Entradas hoy", int(dash["kpis"]["entradas_hoy"]))
col2.metric("Salidas hoy", int(dash["kpis"]["salidas_hoy"]))
col3.metric("Aptos con faltantes (min)", int(dash["kpis"]["aptos_con_faltantes"]))

st.divider()
st.subheader("1) PRIMER PLANO – Entradas diarias (prioridad)")
st.dataframe(dash["primer_plano"], use_container_width=True, height=360)

st.download_button(
    "⬇️ Descargar picking (Primer plano)",
    data=dash["primer_plano_xlsx"],
    file_name=f"Picking_PrimerPlano_{ref_date.isoformat()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

st.divider()
st.subheader("2) ENTRADAS PRÓXIMAS – mismos grupos de las entradas de hoy")
st.dataframe(dash["entradas_proximas"], use_container_width=True, height=320)

st.divider()
st.subheader("3) OCUPADOS con salida próxima – fuera del grupo de entradas")
st.dataframe(dash["ocupados_salida_proxima"], use_container_width=True, height=320)

st.divider()
st.subheader("Control de calidad")
c1, c2 = st.columns(2)
with c1:
    st.markdown("**Apartamentos sin zona (revisar maestro de zonas):**")
    st.dataframe(dash["qc_no_zona"], use_container_width=True, height=220)
with c2:
    st.markdown("**Apartamentos sin almacén (revisar maestro apt↔almacén):**")
    st.dataframe(dash["qc_no_almacen"], use_container_width=True, height=220)

st.markdown("**Productos Odoo sin clasificar (para ampliar reglas):**")
st.dataframe(dash["qc_unclassified_products"], use_container_width=True, height=260)
