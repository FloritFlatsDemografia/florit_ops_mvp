# src/loaders.py
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import pandas as pd


def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = (
        s.replace("ó", "o")
        .replace("í", "i")
        .replace("á", "a")
        .replace("é", "e")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    s = s.replace(" ", "").replace("_", "").replace("-", "")
    return s


def _pick_file(data_dir: Path, patterns: list[str]) -> Path:
    for pat in patterns:
        hits = sorted(data_dir.glob(pat))
        if hits:
            return hits[0]
    raise FileNotFoundError(
        f"No encuentro archivo en data/ con patrones: {patterns}. "
        f"Archivos actuales: {[p.name for p in sorted(data_dir.iterdir()) if p.is_file()]}"
    )


def _pick_sheet_by_cols(xls: pd.ExcelFile, required_norm_cols: set[str]) -> str:
    best = None
    best_score = -1
    for sh in xls.sheet_names:
        hdr = xls.parse(sheet_name=sh, nrows=0)
        cols = {_norm(c) for c in hdr.columns}
        score = len(required_norm_cols.intersection(cols))
        if score > best_score:
            best_score = score
            best = sh
    if best is None or best_score == 0:
        # fallback primera hoja
        return xls.sheet_names[0]
    return best


def _read_excel_best_sheet(path: Path, required_norm_cols: set[str]) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    sh = _pick_sheet_by_cols(xls, required_norm_cols)
    df = pd.read_excel(path, sheet_name=sh)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _rename(df: pd.DataFrame, mapping_norm_to_std: dict[str, str]) -> pd.DataFrame:
    norm_map = {_norm(c): c for c in df.columns}
    ren = {}
    for nk, std in mapping_norm_to_std.items():
        if nk in norm_map:
            ren[norm_map[nk]] = std
    return df.rename(columns=ren)


@lru_cache(maxsize=1)
def load_masters_repo() -> dict:
    # repo root = parent de /src
    base_dir = Path(__file__).resolve().parents[1]
    data_dir = base_dir / "data"
    if not data_dir.exists():
        raise FileNotFoundError(f"No existe carpeta data/: {data_dir}")

    # 1) Archivos (por patrón)
    apt_file = _pick_file(data_dir, ["*Apartamentos*Inventarios*.xlsx", "*Apartamentos*Inventarios*.xls"])
    cafe_file = _pick_file(data_dir, ["*Cafe*por*apartamento*.xlsx", "*Cafe*por*apartamento*.xls"])
    thr_file = _pick_file(data_dir, ["*Stock*minimo*por*almacen*.xlsx", "*Stock*minimo*por*almacen*.xls"])
    zonas_file = _pick_file(data_dir, ["*Agrupacion*zon*.xlsx", "*Agrupacion*zon*.xls", "*Zonas*.xlsx", "*Zonas*.xls"])

    # 2) Cargar cada maestro (buscando hoja por columnas)
    apt = _read_excel_best_sheet(apt_file, {"apartamento", "almacen"})
    cafe = _read_excel_best_sheet(cafe_file, {"apartamento"})
    zonas = _read_excel_best_sheet(zonas_file, {"apartamento", "zona"})
    thresholds = _read_excel_best_sheet(thr_file, {"amenity"})

    # 3) Normalizar/renombrar columnas esperadas
    # ZONAS
    zonas = _rename(zonas, {"apartamento": "APARTAMENTO", "zona": "ZONA"})
    if "APARTAMENTO" not in zonas.columns or "ZONA" not in zonas.columns:
        raise ValueError(f"ZONAS debe tener APARTAMENTO y ZONA. Columnas: {list(zonas.columns)}")
    zonas["APARTAMENTO"] = zonas["APARTAMENTO"].astype(str).str.strip()

    # CAFE
    cafe = _rename(
        cafe,
        {
            "apartamento": "APARTAMENTO",
            "cafe_tipo": "CAFE_TIPO",
            "cafetipo": "CAFE_TIPO",
            "cafe": "CAFE_TIPO",
            "tipocafe": "CAFE_TIPO",
        },
    )
    if "APARTAMENTO" not in cafe.columns or "CAFE_TIPO" not in cafe.columns:
        raise ValueError(f"CAFE debe tener APARTAMENTO y CAFE_TIPO. Columnas: {list(cafe.columns)}")
    cafe["APARTAMENTO"] = cafe["APARTAMENTO"].astype(str).str.strip()

    # APT_ALMACEN (+ Localizacion)
    apt = _rename(
        apt,
        {
            "apartamento": "APARTAMENTO",
            "almacen": "ALMACEN",
            "localizacion": "Localizacion",
            "localizaciongps": "Localizacion",
            "coordenadas": "Localizacion",
            "coords": "Localizacion",
            "gps": "Localizacion",
        },
    )
    # soporte acento
    if "Localización" in apt.columns and "Localizacion" not in apt.columns:
        apt = apt.rename(columns={"Localización": "Localizacion"})

    if "APARTAMENTO" not in apt.columns or "ALMACEN" not in apt.columns:
        raise ValueError(f"APT_ALMACEN debe tener APARTAMENTO y ALMACEN. Columnas: {list(apt.columns)}")

    if "Localizacion" not in apt.columns:
        apt["Localizacion"] = pd.NA

    apt["APARTAMENTO"] = apt["APARTAMENTO"].astype(str).str.strip()
    apt["ALMACEN"] = apt["ALMACEN"].astype(str).str.strip()

    # THRESHOLDS: no fuerzo estructura; tu summarize_replenishment ya lo gestiona
    thresholds.columns = [str(c).strip() for c in thresholds.columns]
    if "AMENITY" in thresholds.columns and "Amenity" not in thresholds.columns:
        thresholds = thresholds.rename(columns={"AMENITY": "Amenity"})

    return {
        "zonas": zonas,
        "apt_almacen": apt,
        "cafe": cafe,
        "thresholds": thresholds,
    }
