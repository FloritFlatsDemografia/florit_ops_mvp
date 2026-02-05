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
    Devuelve una clave CANÓNICA (estable) para poder cruzar:
    - Odoo -> Producto real (con nombres largos)
    - Maestro thresholds -> Producto/categoría (más corto)
    """
    t = _norm_txt(product_name)

    # --- Café ---
    if "tassimo" in t:
        return "cafe_tassimo"
    if ("dolce" in t and "gusto" in t) or "dolcegusto" in t:
        return "cafe_dolcegusto"
    # En tu maestro aparece "Café en cápsula Colombia" (Nespresso)
    if "nespresso" in t or "capsula colombia" in t or ("capsula" in t and "colombia" in t):
        return "cafe_nespresso"
    if "molido" in t and "cafe" in t:
        return "cafe_molido"

    # --- Amenities / consumibles ---
    if "gel" in t and "duch" in t:
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
    "champu": "Champu Rituals",
    "gel_manos": "Gel de manos",
    "azucar": "Azúcar blanco en sobres",
    "infusion": "Infusión",
    "insecticida": "Insecticida",
    "detergente": "Detergente",
    "vinagre": "Vinagre",
    "abrillantador": "Abrillantador",
    "sal_lavavajillas": "Sal de lavavajillas",
    "sal_mesa": "Sal fina de mesa",
    "escoba": "Escoba",
    "fregona": "Fregona",
}


def normalize_products(odoo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera columnas: Ubicación, Producto, Cantidad (o equivalentes detectadas por parser).
    """
    df = odoo_df.copy()

    # Normaliza nombres de columnas
    df.columns = [c.strip() for c in df.columns]

    # Asegura Producto/Cantidad
    if "Producto" not in df.columns:
        # intentar nombres alternativos típicos
        for alt in ["Product", "Producto ", "product"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Producto"})
                break

    if "Cantidad" not in df.columns:
        for alt in ["Quantity", "Cantidad ", "quantity"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Cantidad"})
                break

    # Quita filas “totales” o vacías
    df = df[df["Producto"].notna()].copy()

    df["AmenityKey"] = df["Producto"].apply(amenity_key)
    df["Amenity"] = df["AmenityKey"].map(DISPLAY_BY_KEY)

    return df


def _clean_thresholds(thresholds: pd.DataFrame) -> pd.DataFrame:
    thr = thresholds.copy()
    thr.columns = [c.strip() for c in thr.columns]

    # Columna producto puede venir como "Producto " (con espacio)
    prod_col = None
    for c in thr.columns:
        if _norm_txt(c) in ["producto", "product"]:
            prod_col = c
            break
    if prod_col is None:
        raise ValueError(f"Thresholds: no encuentro columna Producto. Columnas: {list(thr.columns)}")

    # Min/Max con posibles acentos
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

    # Si alguna fila no mapea, no rompe: se queda sin key y no se aplicará.
    thr["Minimo"] = pd.to_numeric(thr["Minimo"], errors="coerce").fillna(0)
    thr["Maximo"] = pd.to_numeric(thr["Maximo"], errors="coerce").fillna(0)

    # Normaliza display por key (si el maestro trae “Gel ducha”, etc.)
    thr["Amenity"] = thr["AmenityKey"].map(DISPLAY_BY_KEY)
    return thr[["AmenityKey", "Amenity", "Minimo", "Maximo", "Producto"]].dropna(subset=["AmenityKey"])


def summarize_replenishment(
    stock_by_alm: pd.DataFrame,
    thresholds: pd.DataFrame,
    objective: str = "max",          # "max" o "min"
    urgent_only: bool = False,       # si True, solo filas con Cantidad < Minimo (pero cantidad a llevar sigue objetivo)
) -> pd.DataFrame:
    """
    stock_by_alm: columnas esperadas: ALMACEN, AmenityKey, Cantidad
    thresholds: maestro con Producto/Minimo/Maximo (lo normalizamos internamente)

    Devuelve:
    - Cantidad (stock)
    - Minimo/Maximo
    - A_reponer_max, A_reponer_min
    - A_reponer (según objective)
    - Faltante_min (flag urgente)
    """
    thr = _clean_thresholds(thresholds)

    out = stock_by_alm.merge(thr, on="AmenityKey", how="left")

    # Si no hay thresholds para esa key, no reponemos (Min/Max = 0)
    out["Minimo"] = out["Minimo"].fillna(0)
    out["Maximo"] = out["Maximo"].fillna(0)

    # Cantidad negativa -> la tratamos como 0 para cálculo (en reposición, -4 equivale a “0 en estantería”)
    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0)
    qty = out["Cantidad"].clip(lower=0)

    out["Faltante_min"] = qty < out["Minimo"]

    out["A_reponer_min"] = (out["Minimo"] - qty).clip(lower=0)
    out["A_reponer_max"] = (out["Maximo"] - qty).clip(lower=0)

    if objective == "min":
        out["A_reponer"] = out["A_reponer_min"]
    else:
        out["A_reponer"] = out["A_reponer_max"]

    if urgent_only:
        out = out[out["Faltante_min"]].copy()

    return out
