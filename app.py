# app.py (COMPLETO) — incluye:
# - Base_apts desde masters (para que salgan todos)
# - Sin opción "Mostrar SOLO apartamentos con reposición"
# - KPI Check-ins presenciales (Apolo 29/180/197 + Serranos)
# - Opción 7: Buscador + Ficha de apartamento (con Maps)
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


def build_gmaps_place_url(coord: str):
    """Abre Google Maps centrado en una coordenada 'lat,lng'."""
    if not coord or not isinstance(coord, str) or "," not in coord:
        return None
    url = "https://www.google.com/maps/search/?api=1"
    url += f"&query={quote(coord)}"
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


# =========================
# Normalización aptos (check-ins presenciales)
# =========================
def _norm_apt_name(x: str) -> str:
    """
    Normaliza nombres para casar:
    - mayúsculas
    - espacios
    - APOLO 029 == APOLO 29
    """
    s = "" if x is None else str(x)
    s = s.strip().upper()
    s = re.sub(r"\s+", " ", s)

    m = re.match(r"^(APOLO)\s+0*([0-9]+)$", s)
    if m:
        return f"{m.group(1)} {int(m.group(2))}"
    return s


PRESENTIAL_APTS_RAW = {"Apolo 029", "Apolo 197", "Apolo 180", "Serranos"}
PRESENTIAL_APTS = {_norm_apt_name(a) for a in PRESENTIAL_APTS_RAW}


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
4) Check-ins presenciales + Ficha de apartamento
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
    # Parse diarios
    # =========================
    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)

    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant (archivo vacío o columnas no detectadas).")
        st.stop()

    # =========================
    # Cruces maestros con Avantio
    # =========================
    avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()
    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")

    # apt_almacen (ALMACEN + coords)
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

    # añade almacén/coords a las reservas
    avantio_df = avantio_df.merge(ap_map, on="APARTAMENTO", how="left")

    # =========================
    # Base de apartamentos (desde masters)
    # =========================
    base_apts = masters["zonas"][["APARTAMENTO", "ZONA"]].copy()
    base_apts["APARTAMENTO"] = base_apts["APARTAMENTO"].astype(str).str.strip()
    base_apts["ZONA"] = base_apts["ZONA"].astype(str).str.strip()

    # café
    if "cafe" in masters and "APARTAMENTO" in masters["cafe"].columns:
        cafe = masters["cafe"][["APARTAMENTO", "CAFE_TIPO"]].copy()
        cafe["APARTAMENTO"] = cafe["APARTAMENTO"].astype(str).str.strip()
        base_apts = base_apts.merge(cafe, on="APARTAMENTO", how="left")
    else:
        base_apts["CAFE_TIPO"] = ""

    # almacén + coords
    base_apts = base_apts.merge(ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]], on="APARTAMENTO", how="left")
    base_apts["CAFE_TIPO"] = base_apts["CAFE_TIPO"].fillna("").astype(str)
    base_apts["ALMACEN"] = base_apts["ALMACEN"].fillna("").astype(str)

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

    # 2) reposición usada en pantalla (si urgent_only, solo bajo mínimo)
    rep = summarize_replenishment(
        stock_by_alm,
        masters["thresholds"],
        objective="max",
        urgent_only=urgent_only,
    )

    unclassified = odoo_norm[odoo_norm["AmenityKey"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    # =========================
    # Dashboard
    # =========================
    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        base_apts=base_apts,
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

    # =========================
    # Check-ins presenciales (KPI + detalle)
    # =========================
    foco = dash["period_start"]  # date
    op_foco = dash["operativa"].copy()
    op_foco = op_foco[op_foco["Día"] == foco].copy()
    op_foco["_APT_NORM"] = op_foco["APARTAMENTO"].map(_norm_apt_name)

    mask_presential = op_foco["_APT_NORM"].isin(PRESENTIAL_APTS) & op_foco["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA"])
    presential_df = op_foco[mask_presential].copy()

    st.divider()
    cA, cB = st.columns([1, 3])
    with cA:
        st.metric("Check-ins presenciales (día foco)", int(len(presential_df)))

    with cB:
        if "show_presential" not in st.session_state:
            st.session_state["show_presential"] = False

        col_btn, col_hint = st.columns([1, 4])
        with col_btn:
            if st.button("Ver check-ins presenciales"):
                st.session_state["show_presential"] = not st.session_state["show_presential"]
        with col_hint:
            st.caption("Criterio: Estado = ENTRADA / ENTRADA+SALIDA y aptos: Apolo 29, Apolo 180, Apolo 197, Serranos.")

        if st.session_state["show_presential"]:
            if presential_df.empty:
                st.info("No hay check-ins presenciales en el día foco con esos apartamentos.")
            else:
                show_cols = ["Día", "ZONA", "APARTAMENTO", "Cliente", "Estado", "Próxima Entrada", "Lista_reponer", "Completar con"]
                for c in show_cols:
                    if c not in presential_df.columns:
                        presential_df[c] = ""
                presential_df = presential_df[show_cols].copy()

                st.dataframe(
                    presential_df.sort_values(["ZONA", "APARTAMENTO"]).reset_index(drop=True),
                    use_container_width=True,
                    height=min(420, 40 + 35 * len(presential_df)),
                )

    # =========================
    # 7) Buscador + Ficha de apartamento
    # =========================
    st.divider()
    st.subheader("🔎 Buscador · Ficha de apartamento")

    operativa_all = dash["operativa"].copy()

    # Lista de apartamentos disponibles
    apt_list = sorted([a for a in operativa_all["APARTAMENTO"].dropna().astype(str).str.strip().unique().tolist() if a])

    q = st.text_input("Buscar apartamento", value="", placeholder="Ej: Apolo 197, Serreria 04, Serranos...").strip()

    if q:
        q_norm = q.lower()
        apt_filtered = [a for a in apt_list if q_norm in a.lower()]
    else:
        apt_filtered = apt_list

    colS1, colS2 = st.columns([2, 1])
    with colS1:
        apt_sel = st.selectbox(
            "Selecciona apartamento",
            options=apt_filtered if apt_filtered else apt_list,
            index=0 if (apt_filtered or apt_list) else 0,
        )

    with colS2:
        days_to_show = st.number_input("Días a mostrar (dentro del periodo)", min_value=1, max_value=14, value=2, step=1)

    if not apt_sel:
        st.info("No hay apartamentos disponibles para mostrar.")
    else:
        apt_df = operativa_all[operativa_all["APARTAMENTO"].astype(str).str.strip() == str(apt_sel).strip()].copy()
        apt_df = apt_df.sort_values("Día").reset_index(drop=True)

        foco = dash["period_start"]
        today_row = apt_df[apt_df["Día"] == foco].head(1)

        foco_ts = pd.Timestamp(foco)
        end_ts = foco_ts + pd.Timedelta(days=int(days_to_show) - 1)
        next_days = apt_df[(pd.to_datetime(apt_df["Día"]) >= foco_ts) & (pd.to_datetime(apt_df["Día"]) <= end_ts)].copy()

        coord = None
        try:
            rr = ap_map[ap_map["APARTAMENTO"].astype(str).str.strip() == str(apt_sel).strip()].head(1)
            if not rr.empty:
                coord = _coord_str(rr.iloc[0].get("LAT"), rr.iloc[0].get("LNG"))
        except Exception:
            coord = None

        place_url = build_gmaps_place_url(coord) if coord else None
        dir_url = build_gmaps_directions_url([coord], travelmode="driving", return_to_base=False, optimize=False) if coord else None

        if not today_row.empty:
            r = today_row.iloc[0]
            estado_hoy = str(r.get("Estado", "") or "")
            cliente_hoy = str(r.get("Cliente", "") or "")
            prox_ent = r.get("Próxima Entrada", "")
            cafe = str(r.get("CAFE_TIPO", "") or "")
            lista = str(r.get("Lista_reponer", "") or "")
            completar = str(r.get("Completar con", "") or "")
            zona = str(r.get("ZONA", "") or "")
        else:
            estado_hoy, cliente_hoy, prox_ent, cafe, lista, completar, zona = "", "", "", "", "", "", ""

        cA1, cB1, cC1, cD1 = st.columns([1, 2, 1, 1])
        cA1.metric("Estado HOY", estado_hoy if estado_hoy else "-")
        cB1.metric("Cliente HOY", cliente_hoy if cliente_hoy else "-")
        cC1.metric("Zona", zona if zona else "-")
        cD1.metric("Café", cafe if cafe else "-")

        colL, colR = st.columns([3, 2])
        with colL:
            st.markdown("**Reposición (Lista_reponer)**")
            st.write(lista if lista.strip() else "—")
            st.markdown("**Completar con**")
            st.write(completar if completar.strip() else "—")

        with colR:
            st.markdown("**Próxima Entrada**")
            st.write(str(prox_ent) if str(prox_ent).strip() and str(prox_ent).lower() not in ["none", "nan"] else "—")
            st.markdown("**Mapa**")
            if place_url:
                st.link_button("Ver en Maps", place_url)
            else:
                st.caption("Sin coordenadas (revisar maestro apt_almacen).")
            if dir_url:
                st.link_button("Ruta desde Florit Flats", dir_url)

        st.markdown("**Próximos días (dentro del periodo actual)**")
        show_cols = ["Día", "Estado", "Cliente", "Próxima Entrada", "Lista_reponer", "Completar con"]
        for c in show_cols:
            if c not in next_days.columns:
                next_days[c] = ""
        st.dataframe(
            next_days[show_cols].reset_index(drop=True),
            use_container_width=True,
            height=min(420, 40 + 35 * len(next_days)),
        )

    # =========================
    # Descarga Excel
    # =========================
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

    # Ordena para que los que tienen reposición vayan arriba, sin ocultar el resto
    operativa["__has_rep"] = operativa["Lista_reponer"].astype(str).str.strip().ne("")
    operativa = operativa.sort_values(
        ["Día", "ZONA", "__has_rep", "__prio", "APARTAMENTO"],
        ascending=[True, True, False, True, True],
    )

    for dia, ddf in operativa.groupby("Día", dropna=False):
        st.markdown(f"### Día {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
        if ddf.empty:
            st.info("Sin datos.")
            continue

        for zona, zdf in ddf.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
            st.markdown(f"#### {zona_label}")
            show_df = zdf.drop(columns=["ZONA", "__prio", "__has_rep"], errors="ignore").copy()
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
        colA2, colB2 = st.columns([1, 2])

        with colA2:
            st.markdown("**Totales (preparar carrito)**")
            st.dataframe(totals_df, use_container_width=True, height=min(520, 40 + 35 * len(totals_df)))

        with colB2:
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

    # Para rutas: SOLO con Lista_reponer
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
