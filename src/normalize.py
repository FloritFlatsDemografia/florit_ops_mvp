import pandas as pd
import re
import unicodedata


def _norm_txt(s: str) -> str:
    s = s or ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s


AMENITY_RULES = [
    ("Gel de ducha", [r"gel.*duch", r"gel ducha", r"\bducha\b"]),
    ("Champú", [r"champu", r"shampoo"]),
    ("Jabón de manos", [r"(jabon|gel)\s+de\s+manos", r"hand\s+soap"]),
    ("Azúcar", [r"azucar"]),
    ("Té/Infusión", [r"infus", r"rooibos", r"manzanilla", r"\btilo\b", r"menta", r"earl", r"english"]),
    ("Insecticida", [r"insectic", r"raid", r"mosquit", r"cucarach", r"hormig"]),
    ("Detergente", [r"detergente", r"lavadora", r"capsula.*deterg", r"capsu.*deterg"]),
    ("Vinagre", [r"vinagre"]),
    ("Abrillantador", [r"abrillantador"]),
    ("Sal lavavajillas", [r"sal.*lavavaj"]),
    ("Escoba", [r"escoba"]),
    ("Mocho/Fregona", [r"fregona", r"mocho", r"mopa"]),
]

COFFEE_CAPSULE_RULES = [
    ("Cápsulas Nespresso", [r"nespresso", r"\bcapsul.*nesp"]),
    ("Cápsulas Tassimo", [r"tassimo"]),
    ("Cápsulas Dolce Gusto", [r"dolce\s*gusto", r"dolcegusto", r"\bgusto\b"]),
    ("Cápsulas Senseo", [r"senseo"]),
]


def classify_product(product_name: str):
    t = _norm_txt(product_name)

    for label, patterns in COFFEE_CAPSULE_RULES:
        for p in patterns:
            if re.search(p, t):
                return label

    for label, patterns in AMENITY_RULES:
        for p in patterns:
            if re.search(p, t):
                if label == "Gel de ducha" and re.search(r"manos", t):
                    continue
                return label

    return None


def normalize_products(odoo_df: pd.DataFrame) -> pd.DataFrame:
    df = odoo_df.copy()
    df["Amenity"] = df["Producto"].apply(classify_product)
    return df


def summarize_replenishment(stock_by_alm: pd.DataFrame, thresholds: pd.DataFrame) -> pd.DataFrame:
    thr = thresholds.copy()
    out = stock_by_alm.merge(thr, on="Amenity", how="left")
    out["Minimo"] = out["Minimo"].fillna(0)
    out["Maximo"] = out["Maximo"].fillna(0)

    out["Faltante_min"] = out["Cantidad"] < out["Minimo"]
    out["A_reponer"] = (out["Maximo"] - out["Cantidad"]).clip(lower=0)

    return out
