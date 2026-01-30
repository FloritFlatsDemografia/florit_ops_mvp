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
    from .normalize import classify_product

    df = df_minmax.copy()
    df.columns = [str(c).strip() for c in df.columns]

    col_prod = col_min = col_max = None
    for c in df.columns:
        cl = c.lower()
        if "producto" in cl:
            col_prod = c
        if "min" in cl:
            col_min = c
        if "max" in cl:
            col_max = c

    if not (col_prod and col_min and col_max):
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

    thr = df.groupby("Amenity", as_index=False).agg(
        Minimo=("Minimo", "max"),
        Maximo=("Maximo", "max"),
    )
    return thr


def load_masters_repo() -> dict:
    masters = {}

    missing = [p.name for p in [ZONAS_PATH, APT_ALM_PATH, CAFE_PATH] if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Faltan maestros en data/: {missing}")

    dfz = pd.read_excel(ZONAS_PATH)
    masters["zonas"] = _zones_wide_to_long(dfz)

    dfa = pd.read_excel(APT_ALM_PATH)
    cols = {str(c).strip().lower(): c for c in dfa.columns}
    dfa = dfa.rename(columns={
        cols.get("almacen", "ALMACEN"): "ALMACEN",
        cols.get("apartamento", "APARTAMENTO"): "APARTAMENTO",
    })
    dfa["ALMACEN"] = dfa["ALMACEN"].astype(str).str.strip()
    dfa["APARTAMENTO"] = dfa["APARTAMENTO"].astype(str).str.strip()
    masters["apt_almacen"] = dfa[["ALMACEN", "APARTAMENTO"]].dropna().drop_duplicates()

    dfc = pd.read_excel(CAFE_PATH)
    dfc = dfc.iloc[:, :2].copy()
    dfc.columns = ["APARTAMENTO", "CAFE_TIPO"]
    dfc["APARTAMENTO"] = dfc["APARTAMENTO"].astype(str).str.strip()
    dfc["CAFE_TIPO"] = dfc["CAFE_TIPO"].astype(str).str.strip()
    masters["cafe"] = dfc.dropna().drop_duplicates()

    if THRESHOLDS_PATH.exists():
        df_thr = pd.read_excel(THRESHOLDS_PATH)
        masters["thresholds"] = _build_thresholds_from_stock_minimo(df_thr)
    else:
        from .thresholds import DEFAULT_THRESHOLDS
        masters["thresholds"] = DEFAULT_THRESHOLDS.copy()

    return masters
