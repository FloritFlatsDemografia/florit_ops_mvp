import pandas as pd
from pathlib import Path
import re
import unicodedata

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

ZONAS_PATH = DATA_DIR / "Agrupacion apartamentos por zona.xlsx"
APT_ALM_PATH = DATA_DIR / "Apartamentos e Inventarios.xlsx"
CAFE_PATH = DATA_DIR / "Cafe por apartamento.xlsx"
THRESHOLDS_PATH = DATA_DIR / "Stock minimo por almacen.xlsx"


def _norm(s: str) -> str:
    s = str(s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s


def _zones_wide_to_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    out = []
    for col in df_wide.columns:
        zona = str(col).strip()
        s = df_wide[col].dropna().astype(str).str.strip()
        s = s[s != ""]
        for ap in s.tolist():
            out.append({"APARTAMENTO": ap, "ZONA": zona})
    return pd.DataFrame(out).drop_duplicates()


def _classify_threshold_product(prod_name: str) -> str | None:
    """
    Mapeo ESPECÍFICO para tu Excel de thresholds (Producto/Minimo/Maximo),
    no para nombres largos de Odoo.
    """
    t = _norm(prod_name)

    # Café
    if re.search(r"cafe.*molido|cafe natural molido|molido", t):
        return "Café molido"
    if re.search(r"tassimo", t):
        return "Cápsulas Tassimo"
    if re.search(r"dolce\s*gusto|dolcegusto", t):
        return "Cápsulas Dolce Gusto"
    if re.search(r"senseo", t):
        return "Cápsulas Senseo"
    # tu “café en cápsula Colombia” suele ser cápsula tipo Nespresso
    if re.search(r"cafe.*capsul|capsula", t):
        # si no cayó en tassimo/dolce/senseo, lo tratamos como nespresso-compatible
        return "Cápsulas Nespresso"

    # Amenities
    if re.search(r"azucar", t):
        return "Azúcar"
    if re.search(r"infus|te\b|t[eé]\b", t):
        return "Té/Infusión"
    if re.search(r"champu|shampoo", t):
        return "Champú"
    if re.search(r"gel.*duch|gel ducha|\bducha\b", t) and not re.search(r"manos", t):
        return "Gel de ducha"
    if re.search(r"gel.*manos|jabon.*manos|gel de manos|jabon de manos", t):
        return "Jabón de manos"
    if re.search(r"insectic|raid|mosquit|cucarach|hormig", t):
        return "Insecticida"
    if re.search(r"detergente|lavadora|capsula.*deterg|capsu.*deterg", t):
        return "Detergente"
    if re.search(r"vinagre", t):
        return "Vinagre"
    if re.search(r"abrillantador", t):
        return "Abrillantador"
    if re.search(r"sal.*lavavaj", t):
        return "Sal lavavajillas"
    if re.search(r"\bescoba\b", t):
        return "Escoba"
    if re.search(r"fregona|mocho|mopa", t):
        return "Mocho/Fregona"

    # “Sal fina de mesa” -> si la quieres controlar como amenity, la añadimos:
    if re.search(r"sal fina|sal de mesa|\bsal\b", t) and not re.search(r"lavavaj", t):
        return "Sal de mesa"

    return None


def _build_thresholds_from_stock_minimo(df_minmax: pd.DataFrame) -> pd.DataFrame:
    df = df_minmax.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Detectar columnas
    col_prod = col_min = col_max = None
    for c in df.columns:
        cl = _norm(c)
        if "producto" in cl:
            col_prod = c
        if "min" in cl:
            col_min = c
        if "max" in cl:
            col_max = c

    if not (col_prod and col_min and col_max):
        raise ValueError(
            f"Thresholds: no encuentro columnas Producto/Minimo/Maximo. Columnas={list(df.columns)}"
        )

    df = df[[col_prod, col_min, col_max]].rename(columns={
        col_prod: "Producto",
        col_min: "Minimo",
        col_max: "Maximo",
    })

    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Minimo"] = pd.to_numeric(df["Minimo"], errors="coerce")
    df["Maximo"] = pd.to_numeric(df["Maximo"], errors="coerce")

    # Ignorar filas sin números (por ejemplo “Fregona” vacío)
    df = df.dropna(subset=["Minimo", "Maximo"], how="any")

    df["Amenity"] = df["Producto"].apply(_classify_threshold_product)
    df = df.dropna(subset=["Amenity"])

    if df.empty:
        raise ValueError("Thresholds: tras mapear productos no quedó ninguna fila válida.")

    # Si hay duplicados por Amenity, tomamos el máximo (regla conservadora)
    thr = df.groupby("Amenity", as_index=False).agg(
        Minimo=("Minimo", "max"),
        Maximo=("Maximo", "max"),
    )
    return thr


def load_masters_repo() -> dict:
    masters = {}

    missing = [p.name for p in [ZONAS_PATH, APT_ALM_PATH, CAFE_PATH, THRESHOLDS_PATH] if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Faltan maestros en data/: {missing}")

    # Zonas
    dfz = pd.read_excel(ZONAS_PATH)
    masters["zonas"] = _zones_wide_to_long(dfz)

    # Apt ↔ Almacén
    dfa = pd.read_excel(APT_ALM_PATH)
    dfa.columns = [str(c).strip() for c in dfa.columns]
    # Tomamos las dos primeras columnas si no están claros los nombres
    if len(dfa.columns) >= 2:
        # intenta detectar por nombre
        cols_norm = {_norm(c): c for c in dfa.columns}
        c_ap = cols_norm.get("apartamento") or cols_norm.get("apto") or dfa.columns[0]
        c_al = cols_norm.get("almacen") or cols_norm.get("almacén") or dfa.columns[1]
        dfa = dfa.rename(columns={c_ap: "APARTAMENTO", c_al: "ALMACEN"})
    else:
        raise ValueError("Apartamentos e Inventarios: no tiene 2 columnas mínimas.")

    dfa["ALMACEN"] = dfa["ALMACEN"].astype(str).str.strip()
    dfa["APARTAMENTO"] = dfa["APARTAMENTO"].astype(str).str.strip()
    masters["apt_almacen"] = dfa[["ALMACEN", "APARTAMENTO"]].dropna().drop_duplicates()

    # Café por apto
    dfc = pd.read_excel(CAFE_PATH)
    dfc = dfc.iloc[:, :2].copy()
    dfc.columns = ["APARTAMENTO", "CAFE_TIPO"]
    dfc["APARTAMENTO"] = dfc["APARTAMENTO"].astype(str).str.strip()
    dfc["CAFE_TIPO"] = dfc["CAFE_TIPO"].astype(str).str.strip()
    masters["cafe"] = dfc.dropna().drop_duplicates()

    # Thresholds
    dft = pd.read_excel(THRESHOLDS_PATH)
    masters["thresholds"] = _build_thresholds_from_stock_minimo(dft)

    return masters
