import pandas as pd

def _zones_wide_to_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    # Cada columna es una zona; valores son apartamentos
    out = []
    for col in df_wide.columns:
        zona = str(col).strip()
        s = df_wide[col].dropna().astype(str).str.strip()
        s = s[s != ""]
        for ap in s.tolist():
            out.append({"APARTAMENTO": ap, "ZONA": zona})
    return pd.DataFrame(out).drop_duplicates()

def load_masters_from_uploads(zonas_file, apt_alm_file, cafe_file) -> dict:
    from .thresholds import DEFAULT_THRESHOLDS

    masters = {}

    # Zonas
    dfz = pd.read_excel(zonas_file)
    masters["zonas"] = _zones_wide_to_long(dfz)

    # Apt ↔ Almacén
    dfa = pd.read_excel(apt_alm_file)
    cols = {c.lower(): c for c in dfa.columns}
    dfa = dfa.rename(columns={
        cols.get("almacen","ALMACEN"): "ALMACEN",
        cols.get("apartamento","APARTAMENTO"): "APARTAMENTO",
    })
    dfa["ALMACEN"] = dfa["ALMACEN"].astype(str).str.strip()
    dfa["APARTAMENTO"] = dfa["APARTAMENTO"].astype(str).str.strip()
    masters["apt_almacen"] = dfa[["ALMACEN","APARTAMENTO"]].dropna().drop_duplicates()

    # Café por apartamento
    dfc = pd.read_excel(cafe_file)
    # tu archivo tiene 2 columnas pero la cabecera 1ª puede ser engañosa
    dfc = dfc.iloc[:, :2].copy()
    dfc.columns = ["APARTAMENTO", "CAFE_TIPO"]
    dfc["APARTAMENTO"] = dfc["APARTAMENTO"].astype(str).str.strip()
    dfc["CAFE_TIPO"] = dfc["CAFE_TIPO"].astype(str).str.strip()
    masters["cafe"] = dfc.dropna().drop_duplicates()

    # Thresholds (por ahora embebidos; luego lo pasamos a Excel si quieres)
    masters["thresholds"] = DEFAULT_THRESHOLDS.copy()

    return masters
