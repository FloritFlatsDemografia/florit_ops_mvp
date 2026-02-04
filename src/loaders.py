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
    return best or xls.sheet_names[0]


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


def _zonas_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    # columnas = zonas, celdas = apartamentos
    cols = [c for c in df.columns if not str(c).strip().lower().startswith("unnamed")]
    rows = []
    for zcol in cols:
        s = df[zcol].dropna()
        for v in s.tolist():
            apt = str(v).strip()
            if not apt or apt.lower() == "nan":
                continue
            rows.append({"APARTAMENTO": apt, "ZONA": str(zcol).strip()})

    out = pd.DataFrame(rows).drop_duplicates()
    if not out.empty:
        out["ZONA"] = out["ZONA"].str.replace(r"^Zona\s+", "", regex=True).str.strip()
    return out


def _cafe_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Acepta varios formatos:
    1) Tabla: APARTAMENTO / CAFE_TIPO
    2) 2 columnas sin headers claros (ej: col0=apartamento, col1=cafe)
    3) Transpuesto: una fila con cafés y cabeceras como apartamentos (ej: columnas = apartamentos)
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["APARTAMENTO", "CAFE_TIPO"])

    # limpiar columnas "Unnamed"
    df = df.loc[:, [c for c in df.columns if not str(c).strip().lower().startswith("unnamed")]]

    # Intento 1: tabla estándar
    d1 = _rename(df, {"apartamento": "APARTAMENTO", "cafe_tipo": "CAFE_TIPO", "cafetipo": "CAFE_TIPO", "cafe": "CAFE_TIPO", "tipocafe": "CAFE_TIPO"})
    if "APARTAMENTO" in d1.columns and "CAFE_TIPO" in d1.columns:
        out = d1[["APARTAMENTO", "CAFE_TIPO"]].copy()
        out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()
        out["CAFE_TIPO"] = out["CAFE_TIPO"].astype(str).str.strip()
        out = out[out["APARTAMENTO"].ne("") & out["APARTAMENTO"].ne("nan")]
        return out.drop_duplicates()

    # Intento 2: 2 columnas “sin headers”
    if df.shape[1] == 2:
        out = df.copy()
        out.columns = ["APARTAMENTO", "CAFE_TIPO"]
        out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()
        out["CAFE_TIPO"] = out["CAFE_TIPO"].astype(str).str.strip()
        out = out[out["APARTAMENTO"].ne("") & out["APARTAMENTO"].ne("nan")]
        return out.drop_duplicates()

    # Intento 3: transpuesto (cabeceras son apartamentos, primera fila es café)
    # Caso típico: columnas = ['ALFARO','Tassimo'] porque la primera fila se tomó como cabecera -> ya está “mal leído”.
    # Lo arreglamos leyendo de nuevo SIN cabecera si detectamos que las "columnas" parecen datos.
    cols = [str(c).strip() for c in df.columns]
    # Heurística: si no hay ninguna columna que parezca "apartamento/cafe" y hay pocas columnas,
    # probablemente es transpuesto.
    if all(_norm(c) not in ("apartamento", "cafe", "cafetipo", "cafe_tipo", "tipocafe") for c in cols):
        # Convertir de "columns = apartamentos" a long usando la primera fila como valor
        # df.iloc[0] contiene cafés (o algo equivalente)
        first_row = df.iloc[0].tolist() if len(df) > 0 else []
        pairs = []
        for apt, caf in zip(cols, first_row):
            apt_s = str(apt).strip()
            caf_s = str(caf).strip()
            if apt_s and apt_s.lower() != "nan":
                pairs.append({"APARTAMENTO": apt_s, "CAFE_TIPO": caf_s})
        out = pd.DataFrame(pairs)
        out["APARTAMENTO"] = out["APARTAMENTO"].astype(str).str.strip()
        out["CAFE_TIPO"] = out["CAFE_TIPO"].astype(str).str.strip()
        return out.drop_duplicates()

    # fallback: no reconocido
    return pd.DataFrame(columns=["APARTAMENTO", "CAFE_TIPO"])


@lru_cache(maxsize=1)
def load_masters_repo() -> dict:
    base_dir = Path(__file__).resolve().parents[1]
    data_dir = base_dir / "data"
    if not data_dir.exists():
        raise FileNotFoundError(f"No existe carpeta data/: {data_dir}")

    apt_file = _pick_file(data_dir, ["*Apartamentos*Inventarios*.xlsx", "*Apartamentos*Inventarios*.xls"])
    cafe_file = _pick_file(data_dir, ["*Cafe*por*apartamento*.xlsx", "*Cafe*por*apartamento*.xls"])
    thr_file = _pick_file(data_dir, ["*Stock*minimo*por*almacen*.xlsx", "*Stock*minimo*por*almacen*.xls"])
    zonas_file = _pick_file(data_dir, ["*Agrupacion*apartamento*por*z*.xlsx", "*Agrupacion*apartamento*por*z*.xls", "*Zonas*.xlsx", "*Zonas*.xls"])

    apt = _read_excel_best_sheet(apt_file, {"apartamento", "almacen"})
    cafe_raw = _read_excel_best_sheet(cafe_file, {"apartamento"})
    thresholds = _read_excel_best_sheet(thr_file, {"amenity"})
    zonas_raw = _read_excel_best_sheet(zonas_file, {"apartamento", "zona"})

    # -------- ZONAS --------
    zonas_try = _rename(zonas_raw, {"apartamento": "APARTAMENTO", "zona": "ZONA"})
    if "APARTAMENTO" in zonas_try.columns and "ZONA" in zonas_try.columns:
        zonas = zonas_try[["APARTAMENTO", "ZONA"]].copy()
        zonas["APARTAMENTO"] = zonas["APARTAMENTO"].astype(str).str.strip()
        zonas["ZONA"] = zonas["ZONA"].astype(str).str.strip()
    else:
        zonas = _zonas_wide_to_long(zonas_raw)
        if zonas.empty:
            raise ValueError(
                f"ZONAS debe tener APARTAMENTO y ZONA, o venir en formato columnas por zona. "
                f"Columnas: {list(zonas_raw.columns)}"
            )

    # -------- CAFE --------
    cafe = _cafe_to_long(cafe_raw)
    if cafe.empty:
        raise ValueError(f"CAFE no reconocido. Columnas: {list(cafe_raw.columns)}")

    # -------- APT_ALMACEN (+ Localizacion) --------
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
    if "Localización" in apt.columns and "Localizacion" not in apt.columns:
        apt = apt.rename(columns={"Localización": "Localizacion"})

    if "APARTAMENTO" not in apt.columns or "ALMACEN" not in apt.columns:
        raise ValueError(f"APT_ALMACEN debe tener APARTAMENTO y ALMACEN. Columnas: {list(apt.columns)}")

    if "Localizacion" not in apt.columns:
        apt["Localizacion"] = pd.NA

    apt = apt[["APARTAMENTO", "ALMACEN", "Localizacion"]].copy()
    apt["APARTAMENTO"] = apt["APARTAMENTO"].astype(str).str.strip()
    apt["ALMACEN"] = apt["ALMACEN"].astype(str).str.strip()

    # -------- THRESHOLDS --------
    thresholds.columns = [str(c).strip() for c in thresholds.columns]
    if "AMENITY" in thresholds.columns and "Amenity" not in thresholds.columns:
        thresholds = thresholds.rename(columns={"AMENITY": "Amenity"})

    return {
        "zonas": zonas,
        "apt_almacen": apt,
        "cafe": cafe,
        "thresholds": thresholds,
    }
