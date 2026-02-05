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
    t = _norm_txt(product_name)

    # --- Café ---
    if "tassimo" in t:
        return "cafe_tassimo"
    if ("dolce" in t and "gusto" in t) or "dolcegusto" in t:
        return "cafe_dolcegusto"
    if "nespresso" in t or "capsula colombia" in t or ("capsula" in t and "colombia" in t):
        return "cafe_nespresso"
    if "molido" in t and "cafe" in t:
        return "cafe_molido"

    # --- Amenities ---
    if "gel" in t and "duch" in t and "manos" not in t:
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
    "cafe_tassimo": "Capsula Tassimo",
    "cafe_dolcegusto": "Capsulas Dolce Gusto",
    "cafe_nespresso": "Café en cápsula Colombia",
    "cafe_molido": "Café Natural Molido",
    "gel_ducha": "Gel ducha",
    "champu": "Champu",
    "gel_manos": "Gel de manos",
    "azucar": "Azúcar",
    "infusion": "Té/Infusión",
    "insecticida": "Insecticida",
    "detergente": "Detergente",
    "vinagre": "Vinagre",
    "abrillantador": "Abrillantador",
    "sal_lavavajillas": "Sal lavavajillas",
    "sal_mesa": "Sal de mesa",
    "escoba": "Escoba",
    "fregona": "Fregona",
}


def normalize_products(odoo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera columnas: Ubicación/Ubicacion, Producto, Cantidad
    """
    df = odoo_df.copy()
    df.columns = [c.strip() for c in df.columns]

    # Normaliza nombres típicos
    for c in list(df.columns):
        if _norm_txt(c) in ["ubicacion", "ubicación"]:
            df = df.rename(columns={c: "Ubicación"})
        if _norm_txt(c) in ["producto", "product"]:
            df = df.rename(columns={c: "Producto"})
        if _norm_txt(c) in ["cantidad", "quantity"]:
            df = df.rename(columns={c: "Cantidad"})

    if "Producto" not in df.columns or "Cantidad" not in df.columns:
        raise ValueError(f"Odoo: columnas requeridas Producto/Cantidad no detectadas. Columnas: {list(df.columns)}")

    # Quita filas sin producto (totales tipo "AP29/Stock (15)")
    df = df[df["Producto"].notna()].copy()

    df["AmenityKey"] = df["Producto"].apply(amenity_key)
    df["Amenity"] = df["AmenityKey"].map(DISPLAY_BY_KEY)
    return df


def _clean_thresholds(thresholds: pd.DataFrame) -> pd.DataFrame:
    thr = thresholds.copy()
    thr.columns = [c.strip() for c in thr.columns]

    # Producto
    prod_col = None
    for c in thr.columns:
        if _norm_txt(c) in ["producto", "product"]:
            prod_col = c
            break
    if prod_col is None:
        raise ValueError(f"Thresholds: no encuentro columna Producto. Columnas: {list(thr.columns)}")

    # Min/Max
    min_col = None
    max_col = None
    for c in thr.columns:
        cn = _norm_txt(c)
        if cn in ["minimo", "min", "minimum"]:
            min_col = c
        if cn in ["maximo", "max", "maximum"]:
            max_col = c

    if min_col is None or max_col is None:
        raise ValueError(f"Thresholds: deben existir Minimo y Maximo. Columnas: {list(thr.columns)}")

    thr = thr.rename(columns={prod_col: "Producto", min_col: "Minimo", max_col: "Maximo"})
    thr["AmenityKey"] = thr["Producto"].apply(amenity_key)

    thr["Minimo"] = pd.to_numeric(thr["Minimo"], errors="coerce").fillna(0)
    thr["Maximo"] = pd.to_numeric(thr["Maximo"], errors="coerce").fillna(0)
    thr["Amenity"] = thr["AmenityKey"].map(DISPLAY_BY_KEY)

    return thr[["AmenityKey", "Amenity", "Minimo", "Maximo", "Producto"]].dropna(subset=["AmenityKey"])


def summarize_replenishment(
    stock_by_alm: pd.DataFrame,
    thresholds: pd.DataFrame,
    objective: str = "max",
    urgent_only: bool = False,
) -> pd.DataFrame:
    """
    stock_by_alm: ALMACEN, AmenityKey, Cantidad
    thresholds:   Producto, Minimo, Maximo (sin ALMACEN normalmente)
    objective:
      - "max": A_reponer = Maximo - Cantidad
      - "min": A_reponer = Minimo - Cantidad
    urgent_only:
      - True: devuelve solo filas Bajo_minimo == True (pero A_reponer siempre se calcula según objective)
    """
    thr = _clean_thresholds(thresholds)

    out = stock_by_alm.copy()
    out.columns = [c.strip() for c in out.columns]

    # Normaliza nombres típicos
    if "Ubicación" in out.columns and "ALMACEN" not in out.columns:
        out = out.rename(columns={"Ubicación": "ALMACEN"})
    if "Almacen" in out.columns and "ALMACEN" not in out.columns:
        out = out.rename(columns={"Almacen": "ALMACEN"})

    if "AmenityKey" not in out.columns:
        raise ValueError(f"stock_by_alm debe traer AmenityKey. Columnas: {list(out.columns)}")
    if "Cantidad" not in out.columns:
        raise ValueError(f"stock_by_alm debe traer Cantidad. Columnas: {list(out.columns)}")
    if "ALMACEN" not in out.columns:
        raise ValueError(f"stock_by_alm debe traer ALMACEN. Columnas: {list(out.columns)}")

    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0)

    # --- IMPORTANTE: completar grid ALMACEN x AmenityKey para detectar faltantes = 0 ---
    almacenes = out["ALMACEN"].dropna().astype(str).str.strip().unique().tolist()
    keys_thr = thr["AmenityKey"].dropna().unique().tolist()

    grid = pd.MultiIndex.from_product([almacenes, keys_thr], names=["ALMACEN", "AmenityKey"]).to_frame(index=False)

    out = grid.merge(out[["ALMACEN", "AmenityKey", "Cantidad"]], on=["ALMACEN", "AmenityKey"], how="left")
    out["Cantidad"] = out["Cantidad"].fillna(0)

    # Merge thresholds (no hay ALMACEN en tu master actual)
    out = out.merge(thr[["AmenityKey", "Amenity", "Minimo", "Maximo"]].drop_duplicates(), on="AmenityKey", how="left")

    out["Minimo"] = out["Minimo"].fillna(0)
    out["Maximo"] = out["Maximo"].fillna(0)

    out["Faltan_para_min"] = (out["Minimo"] - out["Cantidad"]).clip(lower=0)
    out["Bajo_minimo"] = out["Faltan_para_min"] > 0

    obj = (objective or "max").strip().lower()
    if obj == "min":
        out["A_reponer"] = (out["Minimo"] - out["Cantidad"]).clip(lower=0)
    else:
        out["A_reponer"] = (out["Maximo"] - out["Cantidad"]).clip(lower=0)

    # Si pides "solo urgente", filtramos por bajo mínimo
    if urgent_only:
        out = out[out["Bajo_minimo"]].copy()

    return out
