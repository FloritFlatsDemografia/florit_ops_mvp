import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

DEFAULT_MASTERS = {
    "zonas_path": Path("/mnt/data/Agrupacion apartamentos por zona.xlsx"),
    "apt_almacen_path": Path("/mnt/data/Apartamentos e Inventarios.xlsx"),
    "cafe_path": Path("/mnt/data/Cafe por apartamento.xlsx"),
    "thresholds_path": None,  # embedded defaults in thresholds.py
}

def _zones_wide_to_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    # Each column name is a zone; values are apartment names.
    out = []
    for col in df_wide.columns:
        zona = str(col).strip()
        s = df_wide[col].dropna().astype(str).str.strip()
        s = s[s != ""]
        for ap in s.tolist():
            out.append({"APARTAMENTO": ap, "ZONA": zona})
    return pd.DataFrame(out).drop_duplicates()

def load_masters() -> dict:
    from .thresholds import DEFAULT_THRESHOLDS
    from .cafe_capsules import DEFAULT_CAFE_CAPSULE_RULES

    masters = {}

    # Zonas
    zpath = DEFAULT_MASTERS["zonas_path"]
    dfz = pd.read_excel(zpath)
    masters["zonas"] = _zones_wide_to_long(dfz)

    # Apt ↔ Almacén
    apath = DEFAULT_MASTERS["apt_almacen_path"]
    dfa = pd.read_excel(apath)
    # Try to be forgiving with column names
    cols = {c.lower(): c for c in dfa.columns}
    # Expect ALMACEN and APARTAMENTO (as per your file)
    dfa = dfa.rename(columns={
        cols.get("almacen","ALMACEN"): "ALMACEN",
        cols.get("apartamento","APARTAMENTO"): "APARTAMENTO",
    })
    dfa["ALMACEN"] = dfa["ALMACEN"].astype(str).str.strip()
    dfa["APARTAMENTO"] = dfa["APARTAMENTO"].astype(str).str.strip()
    masters["apt_almacen"] = dfa[["ALMACEN","APARTAMENTO"]].dropna().drop_duplicates()

    # Café por apartamento
    cpath = DEFAULT_MASTERS["cafe_path"]
    dfc = pd.read_excel(cpath)
    # Your file has 2 columns but header of first one is misleading; take first 2 columns.
    if dfc.shape[1] >= 2:
        dfc = dfc.iloc[:, :2].copy()
        dfc.columns = ["APARTAMENTO","CAFE_TIPO"]
    dfc["APARTAMENTO"] = dfc["APARTAMENTO"].astype(str).str.strip()
    dfc["CAFE_TIPO"] = dfc["CAFE_TIPO"].astype(str).str.strip()
    masters["cafe"] = dfc.dropna().drop_duplicates()

    masters["thresholds"] = DEFAULT_THRESHOLDS.copy()
    masters["cafe_capsule_rules"] = DEFAULT_CAFE_CAPSULE_RULES.copy()

    return masters
