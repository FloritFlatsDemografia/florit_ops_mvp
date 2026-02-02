import pandas as pd
from datetime import timedelta
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
    Criterios fijos:
      - HOY = fecha local Europe/Madrid
      - Bloque 1: entradas HOY
      - Bloque 2: entradas desde mañana hasta +7 días (incluido)
      - Bloque 3: ocupados hoy con salida desde mañana hasta +7 días (incluido)

    Columnas (limpias) en los 3 bloques:
      - APARTAMENTO, ZONA, CAFE_TIPO, Fecha entrada hora, Fecha salida hora, Lista_reponer
      (en bloque 3 no mostramos fecha entrada, solo salida)
    """

    df = avantio_df.copy()

    # --- Hoy (Europe/Madrid) ---
    tz = ZoneInfo("Europe/Madrid")
    today = pd.Timestamp.now(tz=tz).normalize().date()
    start = (pd.Timestamp(today) + pd.Timedelta(days=1)).date()      # mañana
    end = (pd.Timestamp(today) + pd.Timedelta(days=7)).date()        # +7 días

    # --- Parse fechas desde columnas visibles ---
    entrada_dt = pd.to_datetime(df.get("Fecha entrada hora"), errors="coerce", dayfirst=True)
    salida_dt = pd.to_datetime(df.get("Fecha salida hora"), errors="coerce", dayfirst=True)

    df["entrada_d"] = entrada_dt.dt.date
    df["salida_d"] = salida_dt.dt.date

    # --- Estados ---
    df["Entra_hoy"] = df["entrada_d"] == today
    df["Entra_prox_7d"] = (df["entrada_d"] >= start) & (df["entrada_d"] <= end)

    df["Ocupado_hoy"] = (df["entrada_d"] <= today) & (today < df["salida_d"])
    df["Sale_prox_7d"] = (df["salida_d"] >= start) & (df["salida_d"] <= end)

    # ---------------------------------------------------------
    # Lista_reponer por ALMACEN (filtrando café por CAFE_TIPO)
    # ---------------------------------------------------------
    rep = replenishment_df.copy()

    if "ALMACEN" in df.columns and "ALMACEN" in rep.columns:
        rep = rep.merge(
            df[["ALMACEN", "CAFE_TIPO"]].drop_duplicates(),
            on="ALMACEN",
            how="left"
        )

    def keep_row(r):
        amen = r.get("Amenity")
        if amen not in COFFEE_AMENITIES:
            return True
        allowed = _allowed_coffee_amenity(r.get("CAFE_TIPO"))
        if allowed is None:
            return True
        return amen == allowed

    # Si no existe Amenity/A_reponer por cualquier motivo, no romper
    if "Amenity" in rep.columns and "A_reponer" in rep.columns:
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
            .apply(lambda s: ", ".join(s.tolist()[:30]))
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
    # BLOQUE 1: Entradas HOY (solo hoy)
    # ---------------------------------------------------------
    entradas_hoy = df[df["Entra_hoy"]].copy()
    entradas_hoy = entradas_hoy[
        ["APART
