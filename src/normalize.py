import pandas as pd
import re
import unicodedata


def _norm_txt(s: str) -> str:
    """
    Normaliza texto para comparar/mergear:
    - lowercase
    - sin tildes
    - espacios normalizados
    """
    s = s or ""
    s = str(s).strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s


# =========================
# Reglas de clasificación
# =========================
AMENITY_RULES = [
    ("Gel de ducha", [r"gel.*duch", r"gel ducha", r"\bducha\b"]),
    ("Champú", [r"champu", r"shampoo"]),
    ("Jabón de manos", [r"(jabon|gel)\s+de\s+manos", r"gel de manos", r"hand\s+soap"]),
    ("Azúcar", [r"azucar"]),

    ("Té/Infusión", [r"infus", r"\bte\b", r"rooibos", r"manzanilla", r"\btilo\b", r"menta", r"earl", r"english"]),
    ("Insecticida", [r"insectic", r"raid", r"mosquit", r"cucarach", r"hormig"]),
    ("Detergente", [r"detergente", r"lavadora", r"capsula.*deterg", r"capsu.*deterg"]),

    ("Papel higiénico", [r"papel\s*higien", r"higienico", r"papel wc", r"\bwc\b"]),
    ("Botella de agua", [r"botella\s*agua", r"\bagua\b.*\b1l\b", r"agua\s*1l"]),
    ("Kit limpieza", [r"kit\s*limpieza", r"kit limpieza"]),

    ("Vinagre", [r"vinagre"]),
    ("Abrillantador", [r"abrillantador"]),
    ("Sal lavavajillas", [r"sal.*lavavaj"]),
    ("Sal de mesa", [r"sal fina", r"sal de mesa"]),
    ("Escoba", [r"escoba"]),
    ("Mocho/Fregona", [r"fregona", r"mocho", r"mopa"]),
]

COFFEE_RULES = [
    ("Café molido", [r"cafe.*molido", r"\bmolido\b", r"cafe natural molido"]),
]

COFFEE_CAPSULE_RULES = [
    ("Cápsulas Nespresso", [r"nespresso", r"\bcapsul.*nesp", r"capsula colombia", r"capsula.*colombia"]),
    ("Cápsulas Tassimo", [r"tassimo"]),
    ("Cápsulas Dolce Gusto", [r"dolce\s*gusto", r"dolcegusto"]),
    ("Cápsulas Senseo", [r"senseo"]),
]


def classify_product(product_name: str) -> str | None:
    t = _norm_txt(product_name)

    # Coffee (molido)
    for label, patterns in COFFEE_RULES:
        for p in patterns:
            if re.search(p, t):
                return label

    # Coffee capsules
    for label, patterns in COFFEE_CAPSULE_RULES:
        for p in patterns:
            if re.search(p, t):
                return label

    # Amenities
    for label, patterns in AMENITY_RULES:
        for p in patterns:
            if re.search(p, t):
                # Evitar que "gel de manos" caiga en "gel de ducha"
                if label == "Gel de ducha" and re.search(r"manos", t):
                    continue
                return label

    return None


def normalize_products(odoo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade columna 'Amenity' clasificando por nombre de producto.
    """
    df = odoo_df.copy()
    if "Producto" not in df.columns:
        raise ValueError(f"Odoo df debe tener columna 'Producto'. Columnas: {list(df.columns)}")

    df["Amenity"] = df["Producto"].apply(classify_product)
    return df


def _normalize_threshold_columns(thr: pd.DataFrame) -> pd.DataFrame:
    """
    Acepta variantes típicas de columnas del maestro:
    - Minimo / Mínimo
    - Maximo / Máximo
    """
    thr = thr.copy()

    rename_map = {}
    if "Mínimo" in thr.columns and "Minimo" not in thr.columns:
        rename_map["Mínimo"] = "Minimo"
    if "Máximo" in thr.columns and "Maximo" not in thr.columns:
        rename_map["Máximo"] = "Maximo"
    if rename_map:
        thr = thr.rename(columns=rename_map)

    required = {"Amenity", "Minimo", "Maximo"}
    missing = required - set(thr.columns)
    if missing:
        raise ValueError(f"Thresholds debe tener {required}. Faltan: {missing}. Columnas: {list(thr.columns)}")

    thr["Minimo"] = pd.to_numeric(thr["Minimo"], errors="coerce")
    thr["Maximo"] = pd.to_numeric(thr["Maximo"], errors="coerce")

    return thr


def summarize_replenishment(stock_by_alm: pd.DataFrame, thresholds: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula reposición por ALMACEN+Amenity usando min/max.

    ✅ Política Florit (la que necesitas):
      - A_reponer = max(0, Maximo - Cantidad)
      - (se repone SIEMPRE hasta máximo, aunque esté por encima del mínimo)

    Además:
      - Merge robusto por clave sin tildes: amenity_key
      - Cantidades negativas en Odoo: para reposición se tratan como 0 (no tiene sentido reponer "menos")
    """
    if stock_by_alm is None or stock_by_alm.empty:
        return pd.DataFrame(columns=["ALMACEN", "Amenity", "Cantidad", "Minimo", "Maximo", "Faltante_min", "A_reponer"])

    out = stock_by_alm.copy()

    required_left = {"ALMACEN", "Amenity", "Cantidad"}
    missing_left = required_left - set(out.columns)
    if missing_left:
        raise ValueError(f"stock_by_alm debe tener {required_left}. Faltan: {missing_left}. Columnas: {list(out.columns)}")

    thr = _normalize_threshold_columns(thresholds)

    # Claves normalizadas para merge (evita fallos por tildes/case)
    out["amenity_key"] = out["Amenity"].apply(_norm_txt)
    thr["amenity_key"] = thr["Amenity"].apply(_norm_txt)

    out = out.merge(
        thr[["amenity_key", "Minimo", "Maximo"]],
        on="amenity_key",
        how="left",
    )

    # Flag para diagnosticar: si no matchea en thresholds, aquí lo verás
    out["thr_missing"] = out["Minimo"].isna() | out["Maximo"].isna()

    # Numéricos
    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0)

    # Operativa: negativos a 0 para calcular reposición
    out["Cantidad_calc"] = out["Cantidad"].clip(lower=0)

    out["Minimo"] = pd.to_numeric(out["Minimo"], errors="coerce").fillna(0)
    out["Maximo"] = pd.to_numeric(out["Maximo"], errors="coerce").fillna(0)

    # Solo informativo (por si lo quieres usar)
    out["Faltante_min"] = out["Cantidad_calc"] < out["Minimo"]

    # ✅ Reposición siempre hasta máximo
    out["A_reponer"] = (out["Maximo"] - out["Cantidad_calc"]).clip(lower=0)

    out = out.drop(columns=["amenity_key"], errors="ignore")
    return out
