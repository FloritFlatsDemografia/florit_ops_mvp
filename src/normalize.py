import pandas as pd
import re
import unicodedata

def _norm_txt(s: str) -> str:
    s = s or ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s

# Ordered rules: first match wins.
AMENITY_RULES = [
    ("Gel de ducha", [r"gel.*duch", r"gel ducha", r"\bducha\b"]),
    ("Champú", [r"champu", r"shampoo"]),
    ("Jabón de manos", [r"(jabon|gel)\s+de\s+manos", r"hand\s+soap"]),
    ("Azúcar", [r"azucar"]),
    ("Té/Infusión", [r"infus", r"rooibos", r"manzanilla", r"\btilo\b", r"menta", r"earl", r"english"]),
    ("Insecticida", [r"insectic", r"raid", r"mosquit", r"cucarach", r"hormig"]),
    ("Detergente", [r"detergente", r"lavadora"]),
    ("Vinagre", [r"vinagre"]),
    ("Abrillantador", [r"abrillantador"]),
    ("Sal lavavajillas", [r"sal.*lavavaj"]),
    ("Escoba", [r"escoba"]),
    ("Mocho/Fregona", [r"fregona", r"mocho", r"mopa"]),
]

# Coffee capsules: normalize to a separate amenity key so min/max can be per system.
COFFEE_CAPSULE_RULES = [
    ("Cápsulas Nespresso", [r"nespresso", r"\bcapsul.*nesp"]),
    ("Cápsulas Tassimo", [r"tassimo"]),
    ("Cápsulas Dolce Gusto", [r"dolce\s*gusto", r"gusto"]),
    ("Cápsulas Senseo", [r"senseo"]),
]

def classify_product(product_name: str) -> str | None:
    t = _norm_txt(product_name)

    # Coffee capsules first (so they don't get misclassified)
    for label, patterns in COFFEE_CAPSULE_RULES:
        for p in patterns:
            if re.search(p, t):
                return label

    for label, patterns in AMENITY_RULES:
        for p in patterns:
            if re.search(p, t):
                # Avoid false positives: "ducha" shouldn't steal hand soap, etc.
                if label == "Gel de ducha" and re.search(r"manos", t):
                    continue
                return label

    return None

def normalize_products(odoo_df: pd.DataFrame) -> pd.DataFrame:
    df = odoo_df.copy()
    df["Amenity"] = df["Producto"].apply(classify_product)
    return df

def summarize_replenishment(stock_by_alm: pd.DataFrame, thresholds: pd.DataFrame, cafe_capsule_rules: pd.DataFrame) -> pd.DataFrame:
    """
    stock_by_alm columns: ALMACEN, Amenity, Cantidad
    thresholds columns: Amenity, Minimo, Maximo
    """
    thr = thresholds.copy()
    out = stock_by_alm.merge(thr, on="Amenity", how="left")
    out["Minimo"] = out["Minimo"].fillna(0)
    out["Maximo"] = out["Maximo"].fillna(0)
    out["Faltante_min"] = out["Cantidad"] < out["Minimo"]
    out["A_reponer"] = (out["Maximo"] - out["Cantidad"]).clip(lower=0)
    return out
