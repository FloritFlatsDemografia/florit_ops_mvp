import re
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


# =========================
# Estilos tabla operativa
# =========================
def _style_operativa(df: pd.DataFrame):
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


# =========================
# Parse Lista_reponer
# =========================
_RE_ITEM = re.compile(r"^(?P<name>.+?)(?:\s*[xX]\s*(?P<qty>\d+))?$")

def _parse_listareponer(s: str):
    """
    Espera algo tipo: "Detergente x3, Insecticida x1, Té/Infusión x2"
    Devuelve lista de (producto, qty:int)
    """
    if not isinstance(s, str):
        return []
    s = s.strip()
    if not s:
        return []

    items = []
    for raw in s.split(","):
        t = raw.strip()
        if not t:
            continue
        m = _RE_ITEM.match(t)
        if not m:
            continue
        name = (m.group("name") or "").strip()
        qty = m.group("qty")
        if not name:
            continue
        q = int(qty) if qty else 1
        items.append((name, q))
    return items


def _ensure_date(x):
    try:
        return pd.to_datetime(x).date()
    except Exception:
        return None


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

✅ Resultado:
1) **PARTE OPERATIVO** (prioridad absoluta) con reposición visible.
2) **Sugerencia de Reposición** (totales + dónde llevarlo, por ZONA) según el rango (Inicio + Nº días).
"""
        )

    # -------------------------
    # Sidebar: archivos
    # -------------------------
    st.sidebar.header("Archivos diarios")
    avantio_file = st.sidebar.file_uploader(
        "Avantio (Entradas) .xls/.xlsx/.csv",
        type=["xls", "xlsx", "csv", "html"],
    )
    odoo_file = st.sidebar.file_uploader(
        "Odoo (stock.quant) .xlsx/.csv",
        type=["xlsx", "csv"],
    )

    # -------------------------
    # Sidebar: avanzado (opcional)
    # -------------------------
    tz = ZoneInfo("Europe/Madrid")
    today = pd.Timestamp.now(tz=tz).normalize().date()

    with st.sidebar.expander("Avanzado (opcional)", expanded=False):
        st.subheader("Periodo operativo")
        period_start = st.date_input("Inicio", value=today)
        period_days = st.number_input("Nº días", min_value=1, max_value=14, value=2, step=1)

        st.divider()
        only_replenishment = st.checkbox("Mostrar SOLO apartamentos con reposición", value=True)

        st.divider()
        st.subheader("Ruta (extra)")
        travelmode = st.selectbox("Modo", ["walking", "driving"], index=0)
        return_to_base = st.checkbox("Volver a Florit Flats al final", value=False)

    # Defaults si expander no tocado
    if "period_start" not in locals():
        period_start = today
    if "period_days" not in locals():
        period_days = 2
    if "only_replenishment" not in locals():
        only_replenishment = True
    if "travelmode" not in locals():
        travelmode = "walking"
    if "return_to_base" not in locals():
        return_to_base = False

    # -------------------------
    # Carga maestros
    # -------------------------
    masters = load_masters_repo()
    st.sidebar.success("Maestros cargados ✅")

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

    # soporta Localizacion / Localización / Localiación
    if "Localizacion" not in apt_master.columns:
        for alt in ["Localización", "Localiación", "LOCALIZACION", "LOCALIZACIÓN"]:
            if alt in apt_master.columns:
                apt_master = apt_master.rename(columns={alt: "Localizacion"})
                break

    # mínimo requerido
    if "APARTAMENTO" not in apt_master.columns or "ALMACEN" not in apt_master.columns:
        st.error(f"El maestro APT↔ALMACÉN debe tener APARTAMENTO y ALMACEN. Columnas: {list(apt_master.columns)}")
        st.stop()

    if "Localizacion" not in apt_master.columns:
        apt_master["Localizacion"] = ""

    ap_map = apt_master[["APARTAMENTO", "ALMACEN", "Localizacion"]].dropna(subset=["APARTAMENTO"]).drop_duplicates().copy()
    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()

    # Parse Localizacion -> LAT/LNG (robusto aunque falte o venga mal)
    loc = ap_map["Localizacion"].astype(str).str.replace(" ", "", regex=False)
    parts = loc.str.split(",", n=1, expand=True)
    if parts.shape[1] < 2:
        ap_map["LAT"] = pd.NA
        ap_map["LNG"] = pd.NA
    else:
        ap_map["LAT"] = pd.to_numeric(parts.iloc[:, 0], errors="coerce")
        ap_map["LNG"] = pd.to_numeric(parts.iloc[:, 1], errors="coerce")

    # ---------- Avantio -> APARTAMENTO ----------
    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()

    # Cruces maestros
    avantio_df = avantio_df.merge(masters.get("zonas", pd.DataFrame(columns=["APARTAMENTO", "ZONA"])), on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters.get("cafe", pd.DataFrame(columns=["APARTAMENTO", "CAFE_TIPO"])), on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]], on="APARTAMENTO", how="left")

    # ---------- Odoo -> ALMACEN ----------
    odoo_norm = odoo_norm.rename(columns={"Ubicación": "ALMACEN"})
    if "ALMACEN" not in odoo_norm.columns:
        st.error(f"No encuentro columna 'Ubicación' en Odoo normalizado. Columnas: {list(odoo_norm.columns)}")
        st.stop()
    odoo_norm["ALMACEN"] = odoo_norm["ALMACEN"].astype(str).str.strip()

    # Stock por almacén + amenity
    if "Amenity" not in odoo_norm.columns:
        st.error(f"Odoo normalizado no trae columna 'Amenity'. Columnas: {list(odoo_norm.columns)}")
        st.stop()

    stock_by_alm = (
        odoo_norm.groupby(["ALMACEN", "Amenity"], as_index=False)["Cantidad"]
        .sum()
        .rename(columns={"Cantidad": "Cantidad"})
    )

    # Reposición min/max
    rep = summarize_replenishment(stock_by_alm, masters["thresholds"])

    # Productos sin clasificar (por si luego quieres mostrarlo)
    unclassified = odoo_norm[odoo_norm["Amenity"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy() if "Producto" in odoo_norm.columns else pd.DataFrame()

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

    # =========================
    # BLOQUE 1 (PRIORIDAD):
    # PARTE OPERATIVO
    # =========================
    st.divider()
    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

    operativa = dash["operativa"].copy()

    # normaliza fechas
    if "Día" in operativa.columns:
        operativa["Día"] = operativa["Día"].apply(_ensure_date)

    # Filtro solo con reposición (opcional)
    if only_replenishment and "Lista_reponer" in operativa.columns:
        operativa = operativa[operativa["Lista_reponer"].astype(str).str.strip().ne("")].copy()

    # Orden global
    operativa = operativa.sort_values(["Día", "ZONA", "__prio", "APARTAMENTO"], ascending=[True, True, True, True])

    # Mostrar por día y por zona
    for dia, ddf in operativa.groupby("Día", dropna=False):
        if dia is None:
            continue
        st.markdown(f"### Día {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
        if ddf.empty:
            st.info("Sin datos.")
            continue

        for zona, zdf in ddf.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
            st.markdown(f"#### {zona_label}")

            show_df = zdf.drop(columns=["ZONA", "__prio"], errors="ignore").copy()

            # Reordenar columnas para que reposición sea MUY visible
            preferred = [
                "APARTAMENTO",
                "Estado",
                "Entrada hora",
                "Salida hora",
                "Próxima Entrada",
                "Lista_reponer",
                "CAFE_TIPO",
            ]
            cols = [c for c in preferred if c in show_df.columns] + [c for c in show_df.columns if c not in preferred]
            show_df = show_df[cols]

            st.dataframe(
                _style_operativa(show_df),
                use_container_width=True,
                height=min(520, 40 + 35 * len(show_df)),
            )

    # =========================
    # BLOQUE 2:
    # SUGERENCIA DE REPOSICIÓN
    # =========================
    st.divider()
    st.subheader("Sugerencia de Reposición")
    st.caption("Totales por producto + dónde llevarlo (por ZONA). Incluye ENTRADA, ENTRADA+SALIDA y VACÍO dentro del periodo.")

    if operativa.empty or "Lista_reponer" not in operativa.columns or "Estado" not in operativa.columns:
        st.info("No hay datos suficientes para generar la sugerencia (faltan columnas o el dataframe está vacío).")
    else:
        # criterio: entrada / entrada+salida / vacío, y con reposición
        crit = operativa.copy()
        crit = crit[crit["Lista_reponer"].astype(str).str.strip().ne("")].copy()
        crit = crit[crit["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA", "VACIO"])].copy()

        # selector de ZONAS
        zonas_disp = sorted([z for z in crit["ZONA"].fillna("Sin zona").unique().tolist()])
        if not zonas_disp:
            st.info("No hay zonas disponibles para sugerencia.")
        else:
            selected_zones = st.multiselect(
                "Zonas a incluir",
                options=zonas_disp,
                default=zonas_disp,
            )

            if not selected_zones:
                st.info("Selecciona al menos una zona.")
            else:
                crit["ZONA"] = crit["ZONA"].fillna("Sin zona")
                crit = crit[crit["ZONA"].isin(selected_zones)].copy()

                if crit.empty:
                    st.info("Con esas zonas no hay apartamentos con reposición y estado (Entrada / Turnover / Vacío).")
                else:
                    # Construir tabla itemizada
                    rows = []
                    for _, r in crit.iterrows():
                        apt = r.get("APARTAMENTO")
                        zona = r.get("ZONA")
                        dia = r.get("Día")
                        estado = r.get("Estado")
                        lista = r.get("Lista_reponer", "")
                        for name, qty in _parse_listareponer(str(lista)):
                            rows.append(
                                {
                                    "Día": dia,
                                    "ZONA": zona,
                                    "APARTAMENTO": apt,
                                    "Estado": estado,
                                    "Producto": name,
                                    "Cantidad": qty,
                                }
                            )

                    items = pd.DataFrame(rows)
                    if items.empty:
                        st.info("No se pudieron parsear productos desde Lista_reponer (revisa el formato 'Producto xN').")
                    else:
                        # Totales globales
                        tot = (
                            items.groupby("Producto", as_index=False)["Cantidad"]
                            .sum()
                            .sort_values("Cantidad", ascending=False)
                        )
                        st.markdown("#### Totales (periodo seleccionado)")
                        st.dataframe(tot, use_container_width=True, height=min(420, 40 + 30 * len(tot)))

                        # Por zona: totales + destinos
                        st.markdown("#### Dónde llevarlo (por ZONA)")
                        for zona in selected_zones:
                            zitems = items[items["ZONA"] == zona].copy()
                            if zitems.empty:
                                continue

                            st.markdown(f"### {zona}")

                            ztot = (
                                zitems.groupby("Producto", as_index=False)["Cantidad"]
                                .sum()
                                .sort_values("Cantidad", ascending=False)
                            )
                            st.dataframe(ztot, use_container_width=True, height=min(380, 40 + 30 * len(ztot)))

                            # Detalle destinos por apartamento
                            detalle = (
                                crit[crit["ZONA"] == zona][["Día", "APARTAMENTO", "Estado", "Próxima Entrada", "Lista_reponer"]]
                                .drop_duplicates()
                                .sort_values(["Día", "APARTAMENTO"])
                            )
                            with st.expander(f"Ver detalle apartamentos · {zona}", expanded=False):
                                st.dataframe(detalle, use_container_width=True, height=min(520, 40 + 35 * len(detalle)))

    # =========================
    # EXTRA: RUTA GOOGLE MAPS
    # (la dejo debajo para no molestar)
    # =========================
    st.divider()
    st.subheader("📍 Ruta Google Maps (extra) · HOY + MAÑANA (por ZONA)")
    st.caption("Criterio: apartamentos con reposición y Estado visible ese día (ENTRADA / ENTRADA+SALIDA / VACIO). Salida: Florit Flats.")

    # Calcula hoy/mañana dentro del periodo
    tomorrow = (pd.Timestamp(today) + pd.Timedelta(days=1)).date()

    if dash.get("operativa") is None or dash["operativa"].empty:
        st.info("No hay operativa para generar ruta.")
        return

    route_df = dash["operativa"].copy()
    route_df["Día"] = route_df["Día"].apply(_ensure_date)

    if "Lista_reponer" in route_df.columns:
        route_df = route_df[route_df["Lista_reponer"].astype(str).str.strip().ne("")].copy()

    if "Estado" in route_df.columns:
        route_df = route_df[route_df["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA", "VACIO"])].copy()

    route_df = route_df[route_df["Día"].isin([today, tomorrow])].copy()

    # merge coords por APARTAMENTO
    route_df = route_df.merge(ap_map[["APARTAMENTO", "LAT", "LNG"]], on="APARTAMENTO", how="left")
    route_df["COORD"] = route_df.apply(
        lambda r: _coord_str(r["LAT"], r["LNG"]) if pd.notna(r.get("LAT")) and pd.notna(r.get("LNG")) else None,
        axis=1
    )

    if route_df.empty:
        st.info("No hay apartamentos para ruta HOY+MAÑANA con el criterio (o faltan listas de reposición).")
    else:
        MAX_STOPS = 20
        for dia, ddf in route_df.groupby("Día", dropna=False):
            if dia is None:
                continue
            st.markdown(f"### {pd.to_datetime(dia).strftime('%d/%m/%Y')}")

            for zona, zdf in ddf.groupby("ZONA", dropna=False):
                zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
                coords = [c for c in zdf["COORD"].tolist() if c]

                if not coords:
                    st.info(f"{zona_label}: sin coordenadas suficientes para generar ruta.")
                    continue

                for idx, chunk in enumerate(chunk_list(coords, MAX_STOPS), start=1):
                    url = build_gmaps_directions_url(chunk, travelmode=travelmode, return_to_base=return_to_base)
                    if url:
                        st.markdown(f"**{zona_label} · Ruta (tramo {idx})**: {url}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
