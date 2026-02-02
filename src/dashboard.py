import pandas as pd
from io import BytesIO
import math
from zoneinfo import ZoneInfo


COFFEE_AMENITIES = {
    "Café molido",
    "Cápsulas Nespresso",
    "Cápsulas Tassimo",
    "Cápsulas Dolce Gusto",
    "Cápsulas Senseo",
}


def _safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, float) and math.isnan(x):
            return ""
    except Exception:
        pass
    return str(x)


def _allowed_coffee_amenity(cafe_tipo) -> str | None:
    t = _safe_str(cafe_tipo).strip().lower()
    if t == "":
        return None
    if "molido" in t:
        return "Café molido"
    if "tassimo" in t:
        return "Cápsulas Tassimo"
    if "dolce" in t or "gusto" in t:
        return "Cápsulas Dolce Gusto"
    if "senseo" in t:
        return "Cápsulas Senseo"
    if "nespresso" in t or "colombia" in t:
        return "Cápsulas Nespresso"
    return None


def build_dashboard_frames(
    avantio_df: pd.DataFrame,
    replenishment_df: pd.DataFrame,
    unclassified_products: pd.DataFrame | None = None,
) -> dict:
    """
    Criterios fijos (sin parámetros en UI):
      - HOY = fecha local Europe/Madrid
      - Bloque 1: entradas HOY
      - Bloque 2: entradas desde mañana hasta +7 días (incluido)
      - Bloque 3 NUEVO: apartamentos LIBRES desde mañana hasta +3 días (incluido),
        agrupables por zona en UI, y SOLO si tienen Lista_reponer

    Bloque 1 y 2 columnas:
      APARTAMENTO, ZONA, CAFE_TIPO, Fecha entrada hora, Fecha salida hora, Lista_reponer

    Bloque 3 columnas:
      ZONA, APARTAMENTO, CAFE_TIPO, Lista_reponer
    """

    df = avantio_df.copy()

    # --- Hoy (Europe/Madrid) ---
    tz = ZoneInfo("Europe/Madrid")
    today = pd.Timestamp.now(tz=tz).normalize().date()

    # Ventanas
    start_7 = (pd.Timestamp(today) + pd.Timedelta(days=1)).date()  # mañana
    end_7 = (pd.Timestamp(today) + pd.Timedelta(days=7)).date()    # +7

    # Bloque 3: mañana..+3 (incluido)
    start_3 = start_7
    end_3 = (pd.Timestamp(today) + pd.Timedelta(days=3)).date()

    # --- Parse fechas ---
    entrada_dt = pd.to_datetime(df.get("Fecha entrada hora"), errors="coerce", dayfirst=True)
    salida_dt = pd.to_datetime(df.get("Fecha salida hora"), errors="coerce", dayfirst=True)

    df["entrada_d"] = entrada_dt.dt.date
    df["salida_d"] = salida_dt.dt.date

    # --- Flags ---
    df["Entra_hoy"] = df["entrada_d"] == today
    df["Entra_prox_7d"] = (df["entrada_d"] >= start_7) & (df["entrada_d"] <= end_7)

    # Ocupado hoy (incluye estancias que empezaron antes)
    df["Ocupado_hoy"] = (df["entrada_d"] <= today) & (today < df["salida_d"])

    # ---------------------------------------------------------
    # Lista_reponer por ALMACEN (filtrando café por CAFE_TIPO)
    # ---------------------------------------------------------
    rep = replenishment_df.copy()

    if "ALMACEN" in df.columns and "ALMACEN" in rep.columns:
        rep = rep.merge(
            df[["ALMACEN", "CAFE_TIPO"]].drop_duplicates(),
            on="ALMACEN",
            how="left",
        )

    if "Amenity" in rep.columns and "A_reponer" in rep.columns:
        def keep_row(r):
            amen = r.get("Amenity")
            if amen not in COFFEE_AMENITIES:
                return True
            allowed = _allowed_coffee_amenity(r.get("CAFE_TIPO"))
            if allowed is None:
                return True
            return amen == allowed

        rep = rep[rep.apply(keep_row, axis=1)].copy()
        rep_items = rep[rep["A_reponer"] > 0].copy()
    else:
        rep_items = pd.DataFrame(columns=["ALMACEN", "Amenity", "A_reponer"])

    if not rep_items.empty:
        rep_items["linea"] = (
            rep_items["Amenity"].astype(str)
            + " x"
            + rep_items["A_reponer"].round(0).astype(int).astype(str)
        )

        rep_items_agg = (
            rep_items.groupby("ALMACEN")["linea"]
            .apply(lambda s: ", ".join(s.tolist()[:60]))
            .reset_index()
            .rename(columns={"linea": "Lista_reponer"})
        )
    else:
        rep_items_agg = pd.DataFrame(columns=["ALMACEN", "Lista_reponer"])

    if "ALMACEN" in df.columns:
        df = df.merge(rep_items_agg, on="ALMACEN", how="left")
    else:
        df["Lista_reponer"] = ""

    df["Lista_reponer"] = df["Lista_reponer"].fillna("")

    # ---------------------------------------------------------
    # BLOQUE 1: Entradas HOY
    # ---------------------------------------------------------
    entradas_hoy = df[df["Entra_hoy"]].copy()
    entradas_hoy = entradas_hoy[
        ["APARTAMENTO", "ZONA", "CAFE_TIPO", "Fecha entrada hora", "Fecha salida hora", "Lista_reponer"]
    ].sort_values(["ZONA", "APARTAMENTO"])

    # ---------------------------------------------------------
    # BLOQUE 2: Entradas PRÓXIMAS (mañana..+7) — sin incluir hoy
    # ---------------------------------------------------------
    entradas_proximas = df[df["Entra_prox_7d"]].copy()
    entradas_proximas = entradas_proximas[
        ["APARTAMENTO", "ZONA", "CAFE_TIPO", "Fecha entrada hora", "Fecha salida hora", "Lista_reponer"]
    ].sort_values(["Fecha entrada hora", "ZONA", "APARTAMENTO"])

    # ---------------------------------------------------------
    # BLOQUE 3 NUEVO: LIBRES (mañana..+3) + con reposición
    #  - "Libre" = NO existe ninguna reserva que solape la ventana
    #  - Solape si: entrada < (end+1) y salida > start
    # ---------------------------------------------------------
    end_3_plus1 = (pd.Timestamp(end_3) + pd.Timedelta(days=1)).date()
    df["solapa_3d"] = (df["entrada_d"] < end_3_plus1) & (df["salida_d"] > start_3)

    ocupados_en_ventana = (
        df[df["solapa_3d"]]
        .groupby("APARTAMENTO", as_index=False)
        .size()[["APARTAMENTO"]]
    )

    # base única por apartamento (con zona/café/lista_reponer ya mapeados)
    base_ap = df.drop_duplicates("APARTAMENTO").copy()

    libres_3d = base_ap.merge(ocupados_en_ventana, on="APARTAMENTO", how="left", indicator=True)
    libres_3d = libres_3d[libres_3d["_merge"] == "left_only"].copy()
    libres_3d.drop(columns=["_merge"], inplace=True)

    # Solo si tienen algo que reponer
    libres_3d["Lista_reponer"] = libres_3d["Lista_reponer"].fillna("")
    libres_3d = libres_3d[libres_3d["Lista_reponer"].astype(str).str.strip().ne("")].copy()

    libres_3d = libres_3d[
        ["ZONA", "APARTAMENTO", "CAFE_TIPO", "Lista_reponer"]
    ].sort_values(["ZONA", "APARTAMENTO"])

    # ---------------------------------------------------------
    # KPIs
    # ---------------------------------------------------------
    kpis = {
        "entradas_hoy": int(df["Entra_hoy"].sum()),
        "entradas_proximas_7d": int(df["Entra_prox_7d"].sum()),
        "libres_reposicion_3d": int(libres_3d["APARTAMENTO"].nunique()),
    }

    # ---------------------------------------------------------
    # Excel export
    # ---------------------------------------------------------
    filename = f"FloritOPS_{today.isoformat()}.xlsx"
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        entradas_hoy.to_excel(writer, index=False, sheet_name="EntradasHoy")
        entradas_proximas.to_excel(writer, index=False, sheet_name="EntradasProximas_7d")
        libres_3d.to_excel(writer, index=False, sheet_name="LibresReposicion_3d")

    return {
        "kpis": kpis,
        "entradas_hoy": entradas_hoy,
        "entradas_proximas": entradas_proximas,
        "libres_reposicion_3d": libres_3d,
        "excel_all": bio.getvalue(),
        "excel_filename": filename,
    }
