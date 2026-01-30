import pandas as pd
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

ZONAS_DEFAULT = DATA_DIR / "Agrupacion apartamentos por zona.xlsx"
APT_ALM_DEFAULT = DATA_DIR / "Apartamentos e Inventarios.xlsx"
CAFE_DEFAULT = DATA_DIR / "Cafe por apartamento.xlsx"


def _zones_wide_to_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    out = []
    for col in df_wide.columns:
        zona = str(col).strip()
        s = df_wide[col].dropna().astype(str).str.strip()
        s = s[s != ""]
        for ap in s.tolist():
            out.append({"APARTAMENTO": ap, "ZONA": zona})
    return pd.DataFrame(out).drop_duplicates()


def load_masters(zonas_file=None, apt_alm_file=None, cafe_file=None) -> dict:
    """
    Si no pasas uploads, carga maestros desde /data del repo (Streamlit Cloud).
    Si pasas uploads, los usa como override (útil para probar cambios).
    """
    from .thresholds import DEFAULT_THRESHOLDS

    masters = {}

    # --- Zonas ---
    if zonas_file is not None:
        dfz = pd.read_excel(zonas_file)
    else:
        dfz = pd.read_excel(ZONAS_DEFAULT)
    masters["zonas"] = _zones_wide_to_long(dfz)

    # --- Apt ↔ Almacén ---
    if apt_alm_file is not None:
        dfa = pd.read_excel(apt_alm_file)
    else:
        dfa = pd.read_excel(APT_ALM_DEFAULT)

    # tolerante a nombres de columnas
    cols = {str(c).strip().lower(): c for c in dfa.columns}
    dfa = dfa.rename(columns={
        cols.get("almacen", "ALMACEN"): "ALMACEN",
        cols.get("apartamento", "APARTAMENTO"): "APARTAMENTO",
    })
    dfa["ALMACEN"] = dfa["ALMACEN"].astype(str).str.strip()
    dfa["APARTAMENTO"] = dfa["APARTAMENTO"].astype(str).str.strip()
    masters["apt_almacen"] = dfa[["ALMACEN", "APARTAMENTO"]].dropna().drop_duplicates()

    # --- Café por apartamento ---
    if cafe_file is not None:
        dfc = pd.read_excel(cafe_file)
    else:
        dfc = pd.read_excel(CAFE_DEFAULT)

    dfc = dfc.iloc[:, :2].copy()
    dfc.columns = ["APARTAMENTO", "CAFE_TIPO"]
    dfc["APARTAMENTO"] = dfc["APARTAMENTO"].astype(str).str.strip()
    dfc["CAFE_TIPO"] = dfc["CAFE_TIPO"].astype(str).str.strip()
    masters["cafe"] = dfc.dropna().drop_duplicates()

    # --- Min/Max (de momento embebido; luego lo pasamos a excel) ---
    masters["thresholds"] = DEFAULT_THRESHOLDS.copy()

    return masters
