from pathlib import Path
import pandas as pd
import re


def _repo_root() -> Path:
    # /mount/src/florit_ops_mvp/src/loaders.py -> root = parents[1]
    return Path(__file__).resolve().parents[1]


def _find_one(data_dir: Path, patterns: list[str]) -> Path | None:
    for pat in patterns:
        hits = list(data_dir.glob(pat))
        if hits:
            return hits[0]
    return None


def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _parse_latlng(x: str):
    """
    Acepta:
    - "39.47, -0.37"
    - "39.47,-0.37,"
    - con espacios, etc.
    """
    if x is None:
        return None, None
    s = str(x)
    nums = re.findall(r"[-+]?\d+\.\d+", s)
    if len(nums) >= 2:
        return float(nums[0]), float(nums[1])
    return None, None


def _load_zonas(path: Path) -> pd.DataFrame:
    z = pd.read_excel(path)
    z = _clean_cols(z)

    # Caso A: ya viene en formato largo
    if set(["APARTAMENTO", "ZONA"]).issubset(set(z.columns)):
        out = z[["APARTAMENTO", "ZONA"]].dropna().copy()
        out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()
        out["ZONA"] = out["ZONA"].astype(str).str.strip()
        return out

    # Caso B: formato ancho (cada columna es una zona)
    # columnas tipo: "Zona Ruzafa", "Apolos", "Otros", ...
    rows = []
    for col in z.columns:
        serie = z[col].dropna().astype(str).str.strip()
        for apt in serie.tolist():
            if not apt or apt.lower() in ["nan", "none"]:
                continue
            zona = str(col).strip()
            zona = zona.replace("Zona ", "").strip()
            rows.append({"APARTAMENTO": apt, "ZONA": zona})

    out = pd.DataFrame(rows)
    if out.empty:
        raise ValueError(f"ZONAS: no pude construir APARTAMENTO/ZONA desde {path.name}. Columnas: {list(z.columns)}")
    return out.drop_duplicates()


def _load_cafe(path: Path) -> pd.DataFrame:
    c = pd.read_excel(path)
    c = _clean_cols(c)

    if set(["APARTAMENTO", "CAFE_TIPO"]).issubset(set(c.columns)):
        out = c[["APARTAMENTO", "CAFE_TIPO"]].dropna().copy()
        out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()
        out["CAFE_TIPO"] = out["CAFE_TIPO"].astype(str).str.strip()
        return out

    # Si viene con headers raros (ej: columnas = ["ALFARO","Tassimo"]) => lo tratamos como 2 columnas genéricas
    if c.shape[1] >= 2:
        out = c.iloc[:, :2].copy()
        out.columns = ["APARTAMENTO", "CAFE_TIPO"]
        out = out.dropna()
        out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()
        out["CAFE_TIPO"] = out["CAFE_TIPO"].astype(str).str.strip()
        return out

    raise ValueError(f"CAFE: debe tener APARTAMENTO y CAFE_TIPO. Columnas: {list(c.columns)}")


def _load_apt_almacen(path: Path) -> pd.DataFrame:
    a = pd.read_excel(path)
    a = _clean_cols(a)

    # Localización viene como: Localización / Localizacion / Localiación / etc.
    loc_col = None
    for col in a.columns:
        if "local" in col.lower():
            loc_col = col
            break

    if loc_col and loc_col != "Localizacion":
        a = a.rename(columns={loc_col: "Localizacion"})

    required = {"ALMACEN", "APARTAMENTO"}
    if not required.issubset(set(a.columns)):
        raise ValueError(f"APT_ALMACEN: faltan columnas {required}. Columnas: {list(a.columns)}")

    out = a[["ALMACEN", "APARTAMENTO"] + (["Localizacion"] if "Localizacion" in a.columns else [])].copy()
    out["ALMACEN"] = out["ALMACEN"].astype(str).str.strip()
    out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()

    if "Localizacion" in out.columns:
        latlng = out["Localizacion"].apply(_parse_latlng)
        out["LAT"] = latlng.apply(lambda t: t[0])
        out["LNG"] = latlng.apply(lambda t: t[1])

    return out.drop_duplicates()


def _load_thresholds(path: Path) -> pd.DataFrame:
    t = pd.read_excel(path)
    t = _clean_cols(t)
    return t


def load_masters_repo() -> dict:
    data_dir = _repo_root() / "data"
    if not data_dir.exists():
        raise FileNotFoundError(f"No existe carpeta data/: {data_dir}")

    zonas_path = _find_one(data_dir, ["Agrupacion*zon*.xlsx", "Agrupacion*.xlsx"])
    apt_path = _find_one(data_dir, ["Apartamentos*Inventarios*.xlsx", "Apartamentos*.xlsx"])
    cafe_path = _find_one(data_dir, ["Cafe*apartamento*.xlsx", "Café*apartamento*.xlsx", "Cafe*.xlsx"])
    thr_path = _find_one(data_dir, ["Stock minimo*almacen*.xlsx", "Stock*minimo*.xlsx", "Stock*.xlsx"])

    if zonas_path is None:
        raise FileNotFoundError("No encuentro en data/ el Excel de ZONAS (Agrupacion*zon*.xlsx).")
    if apt_path is None:
        raise FileNotFoundError("No encuentro en data/ el Excel de Apartamentos e Inventarios.")
    if cafe_path is None:
        raise FileNotFoundError("No encuentro en data/ el Excel de Cafe por apartamento.")
    if thr_path is None:
        raise FileNotFoundError("No encuentro en data/ el Excel de Stock minimo por almacen.")

    zonas = _load_zonas(zonas_path)
    apt_almacen = _load_apt_almacen(apt_path)
    cafe = _load_cafe(cafe_path)
    thresholds = _load_thresholds(thr_path)

    return {
        "zonas": zonas,
        "apt_almacen": apt_almacen,
        "cafe": cafe,
        "thresholds": thresholds,
    }
