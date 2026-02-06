import re
import unicodedata
import pandas as pd


def _norm_txt(x) -> str:
    if x is None:
        return ""
    try:
        s = str(x)
    except Exception:
        return ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s


def amenity_key(product_name: str) -> str | None:
    """
    Clave CANÓNICA para cruzar:
    - Odoo -> Producto real (nombres largos)
    - Thresholds -> Amenity/Producto (corto)
    """
    t = _norm_txt(product_name)

    # --- Café ---
    if "tassimo" in t:
        return "cafe_tassimo"
    if ("dolce" in t and "gusto" in t) or "dolcegusto" in t:
        return "cafe_dolcegusto"
    if "senseo" in t:
        return "cafe_senseo"
    if "nespresso" in t or "capsula colombia" in t or ("capsula" in t and "colombia" in t):
        return "cafe_nespresso"
    if "molido" in t and "cafe" in t:
        return "cafe_molido"

    # --- Amenities / consumibles ---
    if "gel" in t and "duch" in t:
        if "manos" in t:
            return None
        return "gel_ducha"
    if "champu" in t or "shampoo" in t:
        return "champu"
    if (("jabon" in t) or ("gel" in t)) and "manos" in t:
        return "gel_manos"
    if "azucar" in t:
        return "azucar"
    if "infus" in t or re.search(r"\bte\b", t):
        return "infusion"
    if "insectic" in t or "mosquit" in t or "cucarach" in t or "hormig" in t:
        return "insecticida"
    if "deterg" in t or "lavadora" in t:
        return "detergente"
    if "vinagre" in t:
        return "vinagre"
    if "abrillantador" in t:
        return "abrillantador"
    if "sal" in t and "lavavaj" in t:
        return "sal_lavavajillas"
    if "sal" in t:
        return "sal_mesa"
    if "escoba" in t:
        return "escoba"
    if "fregona" in t or "mocho" in t or "mopa" in t:
        return "fregona"

    return None


DISPLAY_BY_KEY = {
    "cafe_tassimo": "Cápsulas Tassimo",
    "cafe_dolcegusto": "Cápsulas Dolce Gusto",
    "cafe_nespresso": "Cápsulas Nespresso",
    "cafe_senseo": "Cápsulas Senseo",
    "cafe_molido": "Café molido",
    "gel_ducha": "Gel de ducha",
    "champu": "Champú",
    "gel_manos": "Jabón de manos",
    "azucar": "Azúcar",
    "infusion": "Té/Infusión",
    "insecticida": "Insecticida",
    "detergente": "Detergente",
    "vinagre": "Vinagre",
    "abrillantador": "Abrillantador",
    "sal_lavavajillas": "Sal lavavajillas",
    "sal_mesa": "Sal de mesa",
    "escoba": "Escoba",
    "fregona": "Mocho/Fregona",
}


def normalize_products(odoo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera columnas: Ubicación, Producto, Cantidad (o equivalentes).
    Devuelve: AmenityKey + Amenity (display)
    """
    df = odoo_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Producto
    if "Producto" not in df.columns:
        for alt in ["Product", "Producto ", "product", "Nombre producto"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Producto"})
                break

    # Cantidad
    if "Cantidad" not in df.columns:
        for alt in ["Quantity", "Cantidad ", "quantity", "Qty"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Cantidad"})
                break

    # Ubicación
    if "Ubicación" not in df.columns and "Ubicacion" in df.columns:
        df = df.rename(columns={"Ubicacion": "Ubicación"})
    if "Ubicación" not in df.columns and "Location" in df.columns:
        df = df.rename(columns={"Location": "Ubicación"})

    df = df[df.get("Producto").notna()].copy()

    df["AmenityKey"] = df["Producto"].apply(amenity_key)
    df["Amenity"] = df["AmenityKey"].map(DISPLAY_BY_KEY)

    if "Cantidad" in df.columns:
        df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0)

    return df


def _clean_thresholds(thresholds: pd.DataFrame) -> pd.DataFrame:
    """
    Acepta thresholds con:
      - Amenity, Minimo, Maximo (tu caso)
      - Producto, Minimo, Maximo
      - AmenityKey, Minimo, Maximo
    Opcional: ALMACEN
    """
    thr = thresholds.copy()
    thr.columns = [str(c).strip() for c in thr.columns]

    # Normaliza nombres de columnas
    col_map = {}
    for c in thr.columns:
        cn = _norm_txt(c)
        if cn in ["minimo", "min", "minimum"]:
            col_map[c] = "Minimo"
        if cn in ["maximo", "max", "maximum"]:
            col_map[c] = "Maximo"
        if cn in ["almacen", "almacén", "ubicacion", "ubicación", "location"]:
            col_map[c] = "ALMACEN"
        if cn in ["producto", "product"]:
            col_map[c] = "Producto"
        if cn in ["amenity", "amenidad"]:
            col_map[c] = "Amenity"
        if cn in ["amenitykey", "amenity_key", "key"]:
            col_map[c] = "AmenityKey"
    if col_map:
        thr = thr.rename(columns=col_map)

    if "Minimo" not in thr.columns:
        thr["Minimo"] = 0
    if "Maximo" not in thr.columns:
        thr["Maximo"] = 0

    # Genera AmenityKey si no existe
    if "AmenityKey" not in thr.columns or thr["AmenityKey"].isna().all():
        if "Producto" in thr.columns:
            thr["AmenityKey"] = thr["Producto"].apply(amenity_key)
        elif "Amenity" in thr.columns:
            thr["AmenityKey"] = thr["Amenity"].apply(amenity_key)
        else:
            thr["AmenityKey"] = None

    thr["Minimo"] = pd.to_numeric(thr["Minimo"], errors="coerce").fillna(0)
    thr["Maximo"] = pd.to_numeric(thr["Maximo"], errors="coerce").fillna(0)

    # Display
    if "Amenity" not in thr.columns or thr["Amenity"].isna().all():
        thr["Amenity"] = thr["AmenityKey"].map(DISPLAY_BY_KEY)
    else:
        thr["Amenity"] = thr["Amenity"].fillna(thr["AmenityKey"].map(DISPLAY_BY_KEY))

    if "ALMACEN" in thr.columns:
        thr["ALMACEN"] = thr["ALMACEN"].astype(str).str.strip()

    thr = thr.dropna(subset=["AmenityKey"]).copy()
    keep = ["AmenityKey", "Amenity", "Minimo", "Maximo"]
    if "ALMACEN" in thr.columns:
        keep = ["ALMACEN"] + keep
    return thr[keep].drop_duplicates()


def summarize_replenishment(
    stock_by_alm: pd.DataFrame,
    thresholds: pd.DataFrame,
    objective: str = "max",
    urgent_only: bool = False,
) -> pd.DataFrame:
    """
    stock_by_alm: ALMACEN, AmenityKey, Cantidad
    thresholds : maestro min/max

    Devuelve por ALMACEN + AmenityKey:
      - Cantidad, Minimo, Maximo
      - Faltan_para_min, Bajo_minimo
      - A_reponer (a máximo por defecto)
    """
    out = stock_by_alm.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if "ALMACEN" not in out.columns:
        for alt in ["Ubicación", "Ubicacion", "Almacen", "Almacén", "Location"]:
            if alt in out.columns:
                out = out.rename(columns={alt: "ALMACEN"})
                break

    if "AmenityKey" not in out.columns:
        if "Amenity" in out.columns:
            out["AmenityKey"] = out["Amenity"].apply(amenity_key)
        else:
            out["AmenityKey"] = None

    if "Cantidad" not in out.columns:
        out["Cantidad"] = 0
    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0)

    thr = _clean_thresholds(thresholds)

    merge_cols = ["AmenityKey"]
    if "ALMACEN" in thr.columns and "ALMACEN" in out.columns:
        merge_cols = ["ALMACEN", "AmenityKey"]

    out = out.merge(thr, on=merge_cols, how="left")
    out["Minimo"] = out["Minimo"].fillna(0)
    out["Maximo"] = out["Maximo"].fillna(0)

    out["Faltan_para_min"] = (out["Minimo"] - out["Cantidad"]).clip(lower=0)
    out["Bajo_minimo"] = out["Faltan_para_min"] > 0

    if objective == "min":
        out["A_reponer"] = out["Faltan_para_min"]
    else:
        out["A_reponer"] = (out["Maximo"] - out["Cantidad"]).clip(lower=0)

    if "Amenity" not in out.columns or out["Amenity"].isna().all():
        out["Amenity"] = out["AmenityKey"].map(DISPLAY_BY_KEY)

    if urgent_only:
        out = out[out["Bajo_minimo"]].copy()

    return out
