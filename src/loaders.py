import pandas as pd
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

ZONAS_PATH = DATA_DIR / "Agrupacion apartamentos por zona.xlsx"
APT_ALM_PATH = DATA_DIR / "Apartamentos e Inventarios.xlsx"
CAFE_PATH = DATA_DIR / "Cafe por apartamento.xlsx"
THRESHOLDS_PATH = DATA_DIR / "Stock minimo por almacen.xlsx"


def _zones_wide_to_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    out = []
    for col in df_wide.columns:
        zona = str(col).strip()
        s = df_wide[col].dropna().astype(str).str.strip()
        s = s[s != ""]
        for ap in s.tolist():
            out.append({"APARTAMENTO": ap, "ZONA": zona})
    return pd.DataFrame(out).drop_duplicates()


def _build_thresholds_from_stock_minimo(df_minmax: pd.DataFrame) -> pd.DataFrame:
    """
    Tu archivo "Stock minimo por almacen.xlsx" viene con:
    Producto | Minimo | Maximo
    pero Producto es nombre largo Odoo.

    Lo convertimos a thresholds por Amenity genérico clasificando por patrones.
    """
    from .normalize import classify_product

    df = df_minmax.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Intentar localizar columnas
    col_prod = None
    col_min = None
    col_max = None
    for c in df.columns:
        cl = c.lower()
        if "producto" in cl:
            col_prod = c
        if "min" in cl:
            col_min = c
        if "max" in cl:
            col_max = c

    if not (col_prod and col_min and col_max):
        # fallback: usa defaults embebidos
        from .thresholds import DEFAULT_THRESHOLDS
        return DEFAULT_THRESHOLDS.copy()

    df = df[[col_prod, col_min, col_max]].rename(columns={
        col_prod: "Producto",
        col_min: "Minimo",
        col_max: "Maximo",
    })

    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Minimo"] = pd.to_numeric(df["Minimo"], errors="coerce")
    df["Maximo"] = pd.to_numeric(df["Maximo"], errors="coerce")

    df["Amenity"] = df["Producto"].apply(classify_product)
    df = df.dropna(subset=["Amenity", "Minimo", "Maximo"])

    if df.empty:
        from .thresholds import DEFAULT_THRESHOLDS
        return DEFAULT_THRESHOLDS.copy()

    # Si hay varias líneas por amenity, tomamos el max de los máximos y el max de los mínimos (conservador)
    thr = df.groupby("Amenity", as_index=False).agg(
        Minimo=("Minimo", "max"),
        Maximo=("Maximo", "max"),
    )

    return thr


def load_masters_repo() -> dict:
    """
    Carga maestros fijos desde la carpeta data/ del repo (Streamlit Cloud).
    """
    masters = {}

    # Validación mínima de existencia (para que el error sea claro)
    missing = [p.name for p in [ZONAS_PATH, APT_ALM_PATH, CAFE_PATH] if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Faltan maestros en data/: {missing}")

    # Zonas
    dfz = pd.read_excel(ZONAS_PATH)
    masters["zonas"] = _zones_wide_to_long(dfz)

    # Apt ↔ Almacén
    dfa = pd.read_excel(APT_ALM_PATH)
    cols = {str(c).strip().lower(): c for c in dfa.columns}
    dfa = dfa.rename(columns={
        cols.get("almacen", "ALMACEN"): "ALMACEN",
        cols.get("apartamento", "APARTAMENTO"): "APARTAMENTO",
    })
    dfa["ALMACEN"] = dfa["ALMACEN"].astype(str).str.strip()
    dfa["APARTAMENTO"] = dfa["APARTAMENTO"].astype(str).str.strip()
    masters["apt_almacen"] = dfa[["ALMACEN", "APARTAMENTO"]].dropna().drop_duplicates()

    # Café
    dfc = pd.read_excel(CAFE_PATH)
    dfc = dfc.iloc[:, :2].copy()
    dfc.columns = ["APARTAMENTO", "CAFE_TIPO"]
    dfc["APARTAMENTO"] = dfc["APARTAMENTO"].astype(str).str.strip()
    dfc["CAFE_TIPO"] = dfc["CAFE_TIPO"].astype(str).str.strip()
    masters["cafe"] = dfc.dropna().drop_duplicates()

    # Thresholds min/max
    if THRESHOLDS_PATH.exists():
        df_thr = pd.read_excel(THRESHOLDS_PATH)
        masters["thresholds"] = _build_thresholds_from_stock_minimo(df_thr)
    else:
        from .thresholds import DEFAULT_THRESHOLDS
        masters["thresholds"] = DEFAULT_THRESHOLDS.copy()

    return masters
