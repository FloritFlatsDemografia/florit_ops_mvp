# src/loaders.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def _repo_root() -> Path:
    # src/loaders.py -> src -> repo root
    return Path(__file__).resolve().parents[1]


def _data_dir() -> Path:
    return _repo_root() / "data"


def _read_excel_first_sheet(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=0, engine="openpyxl")


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {str(c).strip().lower(): str(c).strip() for c in df.columns}
    for cand in candidates:
        k = cand.strip().lower()
        if k in cols:
            return cols[k]
    return None


def _load_zonas(path: Path) -> pd.DataFrame:
    df = _norm_cols(_read_excel_first_sheet(path))

    c_ap = _find_col(df, ["APARTAMENTO", "Apartamento", "APARTAMENTOS"])
    c_z = _find_col(df, ["ZONA", "Zona"])

    # Caso 1: formato largo APARTAMENTO/ZONA
    if c_ap and c_z:
        out = df[[c_ap, c_z]].copy()
        out.columns = ["APARTAMENTO", "ZONA"]
        out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()
        out["ZONA"] = out["ZONA"].astype(str).str.strip()
        out = out[out["APARTAMENTO"].ne("") & out["APARTAMENTO"].ne("nan")]
        return out.drop_duplicates()

    # Caso 2: formato ancho (cada columna es una zona y dentro hay apartamentos)
    rows = []
    for col in df.columns:
        zona = str(col).strip()
        ser = df[col]
        for v in ser.dropna().tolist():
            apt = str(v).strip()
            if apt and apt.lower() != "nan":
                rows.append((apt, zona))
    out = pd.DataFrame(rows, columns=["APARTAMENTO", "ZONA"]).drop_duplicates()

    # Limpieza opcional: quitar prefijo "Zona " si viene así
    out["ZONA"] = out["ZONA"].str.replace(r"^\s*Zona\s+", "", regex=True).str.strip()
    return out


def _load_cafe(path: Path) -> pd.DataFrame:
    df = _norm_cols(_read_excel_first_sheet(path))

    c_ap = _find_col(df, ["APARTAMENTO", "Apartamento"])
    c_cafe = _find_col(df, ["CAFE_TIPO", "Café", "Cafe", "CAFE", "TIPO CAFE", "Tipo cafe", "Tipo Café"])

    # Caso 1: formato correcto
    if c_ap and c_cafe:
        out = df[[c_ap, c_cafe]].copy()
        out.columns = ["APARTAMENTO", "CAFE_TIPO"]
        out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()
        out["CAFE_TIPO"] = out["CAFE_TIPO"].astype(str).str.strip()
        out = out[out["APARTAMENTO"].ne("") & out["APARTAMENTO"].ne("nan")]
        return out.drop_duplicates()

    # Caso 2: el excel viene “sin headers” (pandas interpretó la primera fila como columnas)
    df2 = pd.read_excel(path, sheet_name=0, header=None, engine="openpyxl")
    if df2.shape[1] >= 2:
        out = df2.iloc[:, :2].copy()
        out.columns = ["APARTAMENTO", "CAFE_TIPO"]
        out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()
        out["CAFE_TIPO"] = out["CAFE_TIPO"].astype(str).str.strip()
        out = out[out["APARTAMENTO"].ne("") & out["APARTAMENTO"].ne("nan")]
        return out.drop_duplicates()

    return pd.DataFrame(columns=["APARTAMENTO", "CAFE_TIPO"])


def _load_apt_almacen(path: Path) -> pd.DataFrame:
    df = _norm_cols(_read_excel_first_sheet(path))

    c_alm = _find_col(df, ["ALMACEN", "Almacen", "ALMACÉN", "Almacén"])
    c_ap = _find_col(df, ["APARTAMENTO", "Apartamento"])
    # OJO: tu cabecera real es "Localiación" (errata). Lo soportamos.
    c_loc = _find_col(df, ["Localizacion", "Localización", "Localiación", "LOCALIZACION", "LOCALIZACIÓN", "LOCALIAción", "LOCALIAcion"])

    if not c_alm or not c_ap:
        raise ValueError(
            f"APT↔ALMACÉN: el maestro debe tener ALMACEN y APARTAMENTO. Columnas detectadas: {list(df.columns)}"
        )

    cols = [c_alm, c_ap] + ([c_loc] if c_loc else [])
    out = df[cols].copy()

    out = out.rename(
        columns={
            c_alm: "ALMACEN",
            c_ap: "APARTAMENTO",
            **({c_loc: "Localizacion"} if c_loc else {}),
        }
    )

    out["ALMACEN"] = out["ALMACEN"].astype(str).str.strip()
    out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()

    if "Localizacion" not in out.columns:
        out["Localizacion"] = ""

    out["Localizacion"] = out["Localizacion"].astype(str).str.strip()
    out = out[out["APARTAMENTO"].ne("") & out["APARTAMENTO"].ne("nan")]
    return out.drop_duplicates()


def _load_thresholds(path: Path) -> pd.DataFrame:
    df = _norm_cols(_read_excel_first_sheet(path))

    c_am = _find_col(df, ["Amenity", "AMENITY", "Producto", "PRODUCTO", "Item", "ITEM"])
    c_min = _find_col(df, ["Min", "MIN", "Minimo", "Mínimo", "STOCK_MIN", "MINIMO"])
    c_max = _find_col(df, ["Max", "MAX", "Maximo", "Máximo", "STOCK_MAX", "MAXIMO"])

    if not c_am:
        raise ValueError(f"THRESHOLDS: no encuentro columna Amenity/Producto. Columnas: {list(df.columns)}")

    use_cols = [c_am] + ([c_min] if c_min else []) + ([c_max] if c_max else [])
    out = df[use_cols].copy()
    out = out.rename(columns={c_am: "Amenity", **({c_min: "Min"} if c_min else {}), **({c_max: "Max"} if c_max else {})})

    out["Amenity"] = out["Amenity"].astype(str).str.strip()
    out = out[out["Amenity"].ne("") & out["Amenity"].ne("nan")]
    return out.drop_duplicates()


def load_masters_repo() -> dict:
    d = _data_dir()

    # Ajusta aquí si cambias nombres de archivos
    zonas_path = next(d.glob("Agrupacion*zonas*.xlsx"), None)
    cafe_path = next(d.glob("Cafe*apartamento*.xlsx"), None)
    apt_path = next(d.glob("Apartamentos*Inventarios*.xlsx"), None)
    thr_path = next(d.glob("Stock*minimo*almacen*.xlsx"), None)

    if zonas_path is None:
        raise FileNotFoundError("No encuentro en data/ el Excel de ZONAS (Agrupacion*zonas*.xlsx).")
    if cafe_path is None:
        raise FileNotFoundError("No encuentro en data/ el Excel de CAFE (Cafe*apartamento*.xlsx).")
    if apt_path is None:
        raise FileNotFoundError("No encuentro en data/ el Excel APT↔ALMACEN (Apartamentos*Inventarios*.xlsx).")
    if thr_path is None:
        raise FileNotFoundError("No encuentro en data/ el Excel THRESHOLDS (Stock*minimo*almacen*.xlsx).")

    zonas = _load_zonas(zonas_path)
    cafe = _load_cafe(cafe_path)
    apt_almacen = _load_apt_almacen(apt_path)
    thresholds = _load_thresholds(thr_path)

    return {
        "zonas": zonas,
        "cafe": cafe,
        "apt_almacen": apt_almacen,
        "thresholds": thresholds,
    }
