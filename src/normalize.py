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
    Devuelve una clave CANÓNICA para cruzar Odoo <-> Maestro (thresholds).
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
    Espera columnas típicas Odoo export:
      - Ubicación / Ubicacion / Location
      - Product / Producto
      - Quantity / Cantidad
    """
    df = odoo_df.copy()
    df.columns = [c.strip() for c in df.columns]

    # Producto
    if "Producto" not in df.columns:
        for alt in ["Product", "product", "Producto ", "PRODUCT"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Producto"})
                break

    # Cantidad
    if "Cantidad" not in df.columns:
        for alt in ["Quantity", "quantity", "Cantidad ", "QTY"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Cantidad"})
                break

    # Ubicación
    if "Ubicación" not in df.columns:
        for alt in ["Ubicacion", "Location", "Ubicación ", "UBICACION"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "Ubicación"})
                break

    if "Producto" not in df.columns or "Cantidad" not in df.columns:
        raise ValueError(f"Odoo: no detecto columnas Producto/Cantidad. Columnas: {list(df.columns)}")

    # Limpia filas vacías / cabeceras de grupo (suelen traer Product NaN)
    df = df[df["Producto"].notna()].copy()

    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0)

    df["AmenityKey"] = df["Producto"].apply(amenity_key)
    df["Amenity"] = df["AmenityKey"].map(DISPLAY_BY_KEY)

    return df


def _clean_thresholds(thresholds: pd.DataFrame) -> pd.DataFrame:
    thr = thresholds.copy()
    thr.columns = [c.strip() for c in thr.columns]

    # Producto puede venir como "Producto" o "Producto " (ya strip)
    if "Producto" not in thr.columns:
        # intenta localizar una columna "producto" con normalización
        prod_col = None
        for c in thr.columns:
            if _norm_txt(c) in ["producto", "product"]:
                prod_col = c
                break
        if prod_col is None:
            raise ValueError(f"Thresholds: no encuentro columna Producto. Columnas: {list(thr.columns)}")
        thr = thr.rename(columns={prod_col: "Producto"})

    # Minimo / Maximo (robusto a acentos/casos)
    if "Minimo" not in thr.columns:
        for c in list(thr.columns):
            if _norm_txt(c) in ["minimo", "min", "minimum"]:
                thr = thr.rename(columns={c: "Minimo"})
                break
    if "Maximo" not in thr.columns:
        for c in list(thr.columns):
            if _norm_txt(c) in ["maximo", "max", "maximum"]:
                thr = thr.rename(columns={c: "Maximo"})
                break

    if "Minimo" not in thr.columns or "Maximo" not in thr.columns:
        raise ValueError(f"Thresholds: deben existir Minimo y Maximo. Columnas: {list(thr.columns)}")

    thr["AmenityKey"] = thr["Producto"].apply(amenity_key)
    thr["Amenity"] = thr["AmenityKey"].map(DISPLAY_BY_KEY)

    thr["Minimo"] = pd.to_numeric(thr["Minimo"], errors="coerce").fillna(0)
    thr["Maximo"] = pd.to_numeric(thr["Maximo"], errors="coerce").fillna(0)

    return thr[["AmenityKey", "Amenity", "Minimo", "Maximo", "Producto"]].dropna(subset=["AmenityKey"])


def summarize_replenishment(
    stock_by_alm: pd.DataFrame,
    thresholds: pd.DataFrame,
    objective: str = "max",
    urgent_only: bool = False,
    **_ignored,
) -> pd.DataFrame:
    """
    stock_by_alm debe traer:
      - ALMACEN
      - Cantidad
      - AmenityKey (recomendado) y/o Amenity

    thresholds: maestro con Producto/Minimo/Maximo (y opcionalmente ALMACEN)
    objective:
      - "max": A_reponer = Maximo - Cantidad
      - "min": A_reponer = Minimo - Cantidad (solo para cumplir mínimo)
    urgent_only:
      - True: devuelve SOLO filas bajo mínimo
    """
    if stock_by_alm is None or stock_by_alm.empty:
        return pd.DataFrame(columns=[
            "ALMACEN", "AmenityKey", "Amenity", "Cantidad", "Minimo", "Maximo",
            "Faltan_para_min", "A_reponer_max", "A_reponer", "Bajo_minimo"
        ])

    out = stock_by_alm.copy()

    # Asegura numérico
    if "Cantidad" not in out.columns:
        out["Cantidad"] = 0
    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0)

    # Asegura AmenityKey
    if "AmenityKey" not in out.columns:
        # Intento: derivar desde Amenity (display) -> key
        rev = { _norm_txt(v): k for k, v in DISPLAY_BY_KEY.items() if v }
        if "Amenity" in out.columns:
            out["AmenityKey"] = out["Amenity"].astype(str).apply(_norm_txt).map(rev)
        else:
            out["AmenityKey"] = None

    # Asegura display
    if "Amenity" not in out.columns:
        out["Amenity"] = out["AmenityKey"].map(DISPLAY_BY_KEY)

    # Limpia thresholds
    thr = _clean_thresholds(thresholds)

    # Merge (thresholds puede ser global o por ALMACEN)
    merge_cols = ["AmenityKey"]
    if "ALMACEN" in out.columns and "ALMACEN" in thresholds.columns:
        # si algún día tu maestro trae ALMACEN, lo soporta
        thr2 = thresholds.copy()
        thr2.columns = [c.strip() for c in thr2.columns]
        if "ALMACEN" not in thr2.columns:
            for c in thr2.columns:
                if _norm_txt(c) in ["almacen", "ubicacion", "ubicacion odoo", "location"]:
                    thr2 = thr2.rename(columns={c: "ALMACEN"})
                    break
        # rehacer clean con ALMACEN si aplica
        # (si no trae, seguimos con thr global)
        if "ALMACEN" in thr2.columns:
            thr2 = thr2.rename(columns={c: c.strip() for c in thr2.columns})
            # mínimo soporte: si trae Producto/Minimo/Maximo + ALMACEN
            base_thr = thr2
            if "Producto" not in base_thr.columns:
                for c in base_thr.columns:
                    if _norm_txt(c) in ["producto", "product"]:
                        base_thr = base_thr.rename(columns={c: "Producto"})
                        break
            if "Minimo" not in base_thr.columns:
                for c in base_thr.columns:
                    if _norm_txt(c) in ["minimo", "min", "minimum"]:
                        base_thr = base_thr.rename(columns={c: "Minimo"})
                        break
            if "Maximo" not in base_thr.columns:
                for c in base_thr.columns:
                    if _norm_txt(c) in ["maximo", "max", "maximum"]:
                        base_thr = base_thr.rename(columns={c: "Maximo"})
                        break

            if "Producto" in base_thr.columns and "Minimo" in base_thr.columns and "Maximo" in base_thr.columns:
                base_thr["AmenityKey"] = base_thr["Producto"].apply(amenity_key)
                base_thr["Minimo"] = pd.to_numeric(base_thr["Minimo"], errors="coerce").fillna(0)
                base_thr["Maximo"] = pd.to_numeric(base_thr["Maximo"], errors="coerce").fillna(0)
                base_thr = base_thr.dropna(subset=["AmenityKey", "ALMACEN"])
                thr = base_thr[["ALMACEN", "AmenityKey", "Minimo", "Maximo"]].drop_duplicates()
                merge_cols = ["ALMACEN", "AmenityKey"]

    # Merge final
    out = out.merge(
        thr[merge_cols + ["Minimo", "Maximo"]].drop_duplicates(),
        on=merge_cols,
        how="left",
    )

    out["Minimo"] = out["Minimo"].fillna(0)
    out["Maximo"] = out["Maximo"].fillna(0)

    # Cálculos
    out["Faltan_para_min"] = (out["Minimo"] - out["Cantidad"]).clip(lower=0)
    out["A_reponer_max"] = (out["Maximo"] - out["Cantidad"]).clip(lower=0)
    out["Bajo_minimo"] = out["Faltan_para_min"] > 0

    obj = (objective or "max").strip().lower()
    if obj in ["min", "minimo", "urgent", "urgente"]:
        out["A_reponer"] = out["Faltan_para_min"]
    else:
        out["A_reponer"] = out["A_reponer_max"]

    if urgent_only:
        out = out[out["Bajo_minimo"]].copy()

    return out
