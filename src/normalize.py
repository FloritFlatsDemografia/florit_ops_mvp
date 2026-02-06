from __future__ import annotations

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
      - Odoo (Producto real)
      - Maestro thresholds (Producto/Amenity)
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
    if "sal fina" in t or "sal de mesa" in t:
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
    df = odoo_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Detecta columnas
    col_map = {}
    for c in df.columns:
        cn = _norm_txt(c)
        if cn in {"ubicacion", "ubicación", "almacen", "almacén"}:
            col_map[c] = "Ubicación"
        elif cn in {"producto", "product"}:
            col_map[c] = "Producto"
        elif cn in {"cantidad", "quantity", "qty"}:
            col_map[c] = "Cantidad"
    if col_map:
        df = df.rename(columns=col_map)

    if "Producto" not in df.columns:
        raise ValueError(f"Odoo: no encuentro columna Producto. Columnas: {list(df.columns)}")
    if "Cantidad" not in df.columns:
        raise ValueError(f"Odoo: no encuentro columna Cantidad. Columnas: {list(df.columns)}")
    if "Ubicación" not in df.columns:
        df["Ubicación"] = pd.NA

    df = df[df["Producto"].notna()].copy()

    df["AmenityKey"] = df["Producto"].apply(amenity_key)
    df["Amenity"] = df["AmenityKey"].map(DISPLAY_BY_KEY)
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0.0)

    return df


def _clean_thresholds(thresholds: pd.DataFrame) -> pd.DataFrame:
    """
    Soporta:
      - Excel: Producto/Minimo/Maximo
      - Default: Amenity/Minimo/Maximo
      - Ya normalizado: AmenityKey/Minimo/Maximo
    """
    thr = thresholds.copy()
    thr.columns = [str(c).strip() for c in thr.columns]

    # Si ya viene normalizado
    if {"AmenityKey", "Minimo", "Maximo"}.issubset(thr.columns):
        out = thr.copy()
        if "Amenity" not in out.columns:
            out["Amenity"] = out["AmenityKey"].map(DISPLAY_BY_KEY)
        out["Minimo"] = pd.to_numeric(out["Minimo"], errors="coerce").fillna(0.0)
        out["Maximo"] = pd.to_numeric(out["Maximo"], errors="coerce").fillna(0.0)
        return out[["AmenityKey", "Amenity", "Minimo", "Maximo"]].dropna(subset=["AmenityKey"])

    # Detecta columna nombre: Producto o Amenity
    name_col = None
    for c in thr.columns:
        cn = _norm_txt(c)
        if cn in {"producto", "product", "amenity"}:
            name_col = c
            break
    if name_col is None:
        raise ValueError(f"Thresholds: no encuentro columna Producto/Amenity. Columnas: {list(thr.columns)}")

    # Detecta Min/Max
    min_col = None
    max_col = None
    for c in thr.columns:
        cn = _norm_txt(c)
        if cn in {"minimo", "min"}:
            min_col = c
        elif cn in {"maximo", "max"}:
            max_col = c
    if min_col is None or max_col is None:
        raise ValueError(f"Thresholds: deben existir Minimo y Maximo. Columnas: {list(thr.columns)}")

    out = thr.rename(columns={name_col: "Producto", min_col: "Minimo", max_col: "Maximo"}).copy()

    out["AmenityKey"] = out["Producto"].apply(amenity_key)
    out = out.dropna(subset=["AmenityKey"]).copy()
    out["Amenity"] = out["AmenityKey"].map(DISPLAY_BY_KEY)

    out["Minimo"] = pd.to_numeric(out["Minimo"], errors="coerce").fillna(0.0)
    out["Maximo"] = pd.to_numeric(out["Maximo"], errors="coerce").fillna(0.0)

    return out[["AmenityKey", "Amenity", "Minimo", "Maximo"]]


def summarize_replenishment(
    stock_by_alm: pd.DataFrame,
    thresholds: pd.DataFrame,
    objective: str = "max",
    urgent_only: bool = False,
) -> pd.DataFrame:
    out = stock_by_alm.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if "Cantidad" not in out.columns:
        out["Cantidad"] = 0.0
    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0.0)

    if "AmenityKey" not in out.columns:
        raise ValueError("stock_by_alm debe incluir AmenityKey")

    thr = _clean_thresholds(thresholds)
    thr_small = thr[["AmenityKey", "Minimo", "Maximo"]].drop_duplicates("AmenityKey")

    out = out.merge(thr_small, on="AmenityKey", how="left")
    out["Minimo"] = out["Minimo"].fillna(0.0)
    out["Maximo"] = out["Maximo"].fillna(0.0)

    out["Faltan_para_min"] = (out["Minimo"] - out["Cantidad"]).clip(lower=0)
    out["Bajo_minimo"] = out["Faltan_para_min"] > 0

    obj = str(objective or "max").strip().lower()
    if obj.startswith("min"):
        out["A_reponer"] = out["Faltan_para_min"]
    else:
        out["A_reponer"] = (out["Maximo"] - out["Cantidad"]).clip(lower=0)

    out["Amenity"] = out["AmenityKey"].map(DISPLAY_BY_KEY)

    if urgent_only:
        out = out[out["Bajo_minimo"]].copy()

    return out
