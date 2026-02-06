import re
import unicodedata
import pandas as pd


# ----------------------------
# Normalización texto
# ----------------------------
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


# ----------------------------
# Mapeo Producto -> AmenityKey
# ----------------------------
def amenity_key(product_name: str) -> str | None:
    """
    Devuelve una clave CANÓNICA (estable) para cruzar:
      - Odoo -> Producto real (nombres largos)
      - Thresholds -> Amenity/Producto (más corto)
    """
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

    # --- Amenities / consumibles ---
    # manos primero para no confundir con ducha
    if (("jabon" in t) or ("gel" in t)) and ("manos" in t or "hand" in t):
        return "gel_manos"

    # gel de ducha: si pone ducha/shower/bath ok; si es "gel rituals" sin "ducha", asumimos ducha si NO es manos
    if ("gel" in t and ("duch" in t or "shower" in t or "bath" in t)) or ("gel" in t and "ritual" in t and "manos" not in t):
        return "gel_ducha"

    if "champu" in t or "shampoo" in t:
        return "champu"

    if "azucar" in t:
        return "azucar"

    if "infus" in t or re.search(r"\bte\b", t) or "rooibos" in t or "manzanilla" in t or "tilo" in t:
        return "infusion"

    if "insectic" in t or "mosquit" in t or "cucarach" in t or "hormig" in t or "raid" in t:
        return "insecticida"

    if "deterg" in t or "lavadora" in t:
        return "detergente"

    if "vinagre" in t:
        return "vinagre"

    if "abrillantador" in t:
        return "abrillantador"

    if "sal" in t and "lavavaj" in t:
        return "sal_lavavajillas"

    if "sal" in t and ("mesa" in t or "fina" in t):
        return "sal_mesa"

    if "escoba" in t:
        return "escoba"

    if "fregona" in t or "mocho" in t or "mopa" in t:
        return "fregona"

    return None


# ----------------------------
# Display (bonito) por key
# ----------------------------
DISPLAY_BY_KEY = {
    "cafe_tassimo": "Cápsulas Tassimo",
    "cafe_dolcegusto": "Cápsulas Dolce Gusto",
    "cafe_nespresso": "Cápsulas Nespresso",
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


# ----------------------------
# Odoo -> normalizado
# ----------------------------
def normalize_products(odoo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera columnas típicas: Ubicación, Producto, Cantidad (o equivalentes).
    Devuelve: ... + AmenityKey + Amenity
    """
    df = odoo_df.copy()
    df.columns = [c.strip() for c in df.columns]

    # Producto
    if "Producto" not in df.columns:
        for alt in ["Product", "Producto ", "product", "Nombre", "Nombre producto"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Producto"})
                break

    # Cantidad
    if "Cantidad" not in df.columns:
        for alt in ["Quantity", "Cantidad ", "quantity", "Qty", "On Hand"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Cantidad"})
                break

    # Ubicación -> se renombra en app.py a ALMACEN, aquí no forzamos

    df = df[df["Producto"].notna()].copy()
    df["AmenityKey"] = df["Producto"].apply(amenity_key)
    df["Amenity"] = df["AmenityKey"].map(DISPLAY_BY_KEY)
    return df


# ----------------------------
# Thresholds -> limpio/estable
# ----------------------------
def _clean_thresholds(thresholds: pd.DataFrame) -> pd.DataFrame:
    """
    Acepta cualquiera de estos formatos:
      A) ['Producto', 'Minimo', 'Maximo', ...]
      B) ['Amenity', 'Minimo', 'Maximo', ...]  <-- TU CASO
      C) con acentos: 'Mínimo', 'Máximo'
      D) opcionalmente con ALMACEN para min/max por almacén
    """
    thr = thresholds.copy()
    thr.columns = [c.strip() for c in thr.columns]

    # Detectar columna "texto producto"
    text_col = None
    for c in thr.columns:
        cn = _norm_txt(c)
        if cn in ["producto", "product", "amenity", "item"]:
            text_col = c
            break
    if text_col is None:
        raise ValueError(f"Thresholds: no encuentro columna Producto/Amenity. Columnas: {list(thr.columns)}")

    # Min/Max (tolerante)
    min_col = None
    max_col = None
    for c in thr.columns:
        cn = _norm_txt(c)
        if cn in ["minimo", "min", "minimum"]:
            min_col = c
        if cn in ["maximo", "max", "maximum"]:
            max_col = c

    if min_col is None:
        # si no hay mínimo, asumimos 0
        thr["Minimo"] = 0
        min_col = "Minimo"
    if max_col is None:
        # si no hay máximo, asumimos = mínimo
        thr["Maximo"] = thr[min_col]
        max_col = "Maximo"

    # ALMACEN opcional
    almacen_col = None
    for c in thr.columns:
        cn = _norm_txt(c)
        if cn in ["almacen", "almacén", "ubicacion", "ubicación"]:
            almacen_col = c
            break

    out = pd.DataFrame()
    if almacen_col:
        out["ALMACEN"] = thr[almacen_col].astype(str).str.strip()

    out["__text"] = thr[text_col].astype(str).str.strip()
    out["AmenityKey"] = out["__text"].apply(amenity_key)

    out["Minimo"] = pd.to_numeric(thr[min_col], errors="coerce").fillna(0)
    out["Maximo"] = pd.to_numeric(thr[max_col], errors="coerce").fillna(0)

    out["Amenity"] = out["AmenityKey"].map(DISPLAY_BY_KEY)
    out = out.dropna(subset=["AmenityKey"]).copy()

    keep_cols = ["AmenityKey", "Amenity", "Minimo", "Maximo"]
    if "ALMACEN" in out.columns:
        keep_cols = ["ALMACEN"] + keep_cols

    return out[keep_cols].drop_duplicates()


# ----------------------------
# Reposición por almacén
# ----------------------------
def summarize_replenishment(
    stock_by_alm: pd.DataFrame,
    thresholds: pd.DataFrame,
    objective: str = "max",
    urgent_only: bool = False,
) -> pd.DataFrame:
    """
    stock_by_alm: ALMACEN + (AmenityKey o Amenity) + Cantidad
    thresholds:   (ALMACEN opcional) + (Amenity o Producto) + Minimo + Maximo

    Devuelve por ALMACEN + AmenityKey:
      - Cantidad (stock actual)
      - Minimo / Maximo
      - Faltan_para_min
      - A_reponer (siempre para llegar a máximo)
      - Bajo_minimo / Faltante_min (flags)

    urgent_only=True:
      - SOLO devuelve los que están bajo mínimo,
        pero A_reponer sigue siendo "hasta máximo".
    """
    out = stock_by_alm.copy()

    # Cantidad numérica
    if "Cantidad" not in out.columns:
        out["Cantidad"] = 0
    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0)

    # Asegura AmenityKey en stock
    if "AmenityKey" not in out.columns:
        if "Amenity" in out.columns:
            out["AmenityKey"] = out["Amenity"].apply(amenity_key)
        else:
            out["AmenityKey"] = None

    # Limpia thresholds (ACEPTA Amenity o Producto)
    thr = _clean_thresholds(thresholds)

    # Merge por ALMACEN+AmenityKey si thresholds lo trae; si no, por AmenityKey
    merge_cols = ["AmenityKey"]
    if "ALMACEN" in thr.columns and "ALMACEN" in out.columns:
        merge_cols = ["ALMACEN", "AmenityKey"]

    out = out.merge(
        thr[merge_cols + ["Minimo", "Maximo"]].drop_duplicates(),
        on=merge_cols,
        how="left",
    )

    out["Minimo"] = pd.to_numeric(out["Minimo"], errors="coerce").fillna(0)
    out["Maximo"] = pd.to_numeric(out["Maximo"], errors="coerce").fillna(0)

    # Display
    out["Amenity"] = out["AmenityKey"].map(DISPLAY_BY_KEY)

    # Cálculos
    out["Faltan_para_min"] = (out["Minimo"] - out["Cantidad"]).clip(lower=0)
    out["Bajo_minimo"] = out["Faltan_para_min"] > 0
    out["Faltante_min"] = out["Bajo_minimo"]  # compat

    # Objetivo (de momento solo "max", pero dejamos hook)
    if objective not in ["max"]:
        objective = "max"

    out["A_reponer"] = (out["Maximo"] - out["Cantidad"]).clip(lower=0)

    # Si es modo urgente: solo filas bajo mínimo (pero con A_reponer completo)
    if urgent_only:
        out = out[out["Bajo_minimo"]].copy()

    # Limpieza final
    if "Amenity" in out.columns:
        out["Amenity"] = out["Amenity"].fillna("")

    return out
