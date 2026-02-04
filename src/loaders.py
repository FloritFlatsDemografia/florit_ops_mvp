# src/loaders.py
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import pandas as pd


# ---------------------------
# Normalización de columnas
# ---------------------------
def _norm_col(s: str) -> str:
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


def _safe_columns(df: pd.DataFrame) -> list[str]:
    return [str(c).strip() for c in df.columns]


# ---------------------------
# Lectores (cabecera vs full)
# ---------------------------
def _read_csv_header(path: Path) -> pd.DataFrame:
    # Solo cabecera, rápido
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc, nrows=0)
        except Exception:
            continue
    return pd.read_csv(path, encoding="latin-1", nrows=0, errors="ignore")


def _read_csv_full(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path, encoding="latin-1", errors="ignore")


@dataclass
class TableRef:
    path: Path
    kind: str  # "excel" | "csv"
    sheet: str | None
    cols_norm: set[str]


def _list_data_files(data_dir: Path) -> list[Path]:
    exts = (".xlsx", ".xls", ".csv")
    return sorted([p for p in data_dir.iterdir() if p.is_file() and p.suffix.lower() in exts])


def _index_tables(data_dir: Path) -> list[TableRef]:
    """
    Indexa tablas leyendo SOLO cabeceras:
      - CSV: pd.read_csv(nrows=0)
      - Excel: parse(nrows=0) por hoja
    """
    refs: list[TableRef] = []
    for path in _list_data_files(data_dir):
        suf = path.suffix.lower()
        if suf == ".csv":
            try:
                hdr = _read_csv_header(path)
                cols = {_norm_col(c) for c in _safe_columns(hdr)}
                refs.append(TableRef(path=path, kind="csv", sheet=None, cols_norm=cols))
            except Exception:
                continue
        else:
            try:
                xl = pd.ExcelFile(path)
                for sh in xl.sheet_names:
                    try:
                        hdr = xl.parse(sheet_name=sh, nrows=0)
                        cols = {_norm_col(c) for c in _safe_columns(hdr)}
                        refs.append(TableRef(path=path, kind="excel", sheet=sh, cols_norm=cols))
                    except Exception:
                        continue
            except Exception:
                continue
    return refs


def _score(ref: TableRef, want: str) -> int:
    fn = ref.path.name.lower()
    sh = (ref.sheet or "").lower()

    cols = ref.cols_norm
    has_ap = "apartamento" in cols
    has_al = "almacen" in cols
    has_z = "zona" in cols
    has_amenity = "amenity" in cols

    has_cafe = any(k in cols for k in ("cafetipo", "cafe_tipo", "cafe", "tipocafe"))
    has_min = any(k in cols for k in ("min", "minimo", "stockmin", "stockminimo"))
    has_max = any(k in cols for k in ("max", "maximo", "stockmax", "stockmaximo"))
    has_loc = any(k in cols for k in ("localizacion", "localizaciongps", "gps", "coords", "coordenadas", "lat", "lng"))

    score = 0

    if want == "apt_almacen":
        if has_ap and has_al:
            score += 100
        if has_loc:
            score += 30
        if "invent" in fn or "apart" in fn or "almacen" in fn:
            score += 10
        if "almacen" in sh or "apart" in sh or "invent" in sh:
            score += 5

    elif want == "zonas":
        if has_ap and has_z:
            score += 100
        if "zona" in fn:
            score += 10
        if "zona" in sh:
            score += 5

    elif want == "cafe":
        if has_ap and has_cafe:
            score += 100
        if "cafe" in fn:
            score += 10
        if "cafe" in sh:
            score += 5

    elif want == "thresholds":
        if has_amenity and (has_min or has_max):
            score += 100
        if "threshold" in fn or "stock" in fn or "min" in fn or "max" in fn:
            score += 10
        if "threshold" in sh or "stock" in sh or "min" in sh or "max" in sh:
            score += 5

    return score


def _pick_table(refs: list[TableRef], want: str) -> TableRef | None:
    best = None
    best_score = -1
    for r in refs:
        s = _score(r, want)
        if s > best_score:
            best_score = s
            best = r
    if best_score < 80:
        return None
    return best


def _load_table(ref: TableRef) -> pd.DataFrame:
    if ref.kind == "csv":
        df = _read_csv_full(ref.path)
    else:
        df = pd.read_excel(ref.path, sheet_name=ref.sheet)
    df.columns = _safe_columns(df)
    return df


def _rename_to_standard(df: pd.DataFrame, desired: dict[str, str]) -> pd.DataFrame:
    cols = list(df.columns)
    norm_map = {_norm_col(c): c for c in cols}
    rename = {}
    for nk, std in desired.items():
        if nk in norm_map:
            rename[norm_map[nk]] = std
    return df.rename(columns=rename)


@lru_cache(maxsize=1)
def load_masters_repo() -> dict:
    """
    Devuelve:
      - zonas: APARTAMENTO, ZONA
      - apt_almacen: APARTAMENTO, ALMACEN, Localizacion
      - cafe: APARTAMENTO, CAFE_TIPO
      - thresholds: tabla de mínimos/máximos por Amenity
    """
    base_dir = Path(__file__).resolve().parents[1]  # repo root
    data_dir = base_dir / "data"
    if not data_dir.exists():
        raise FileNotFoundError(f"No existe carpeta data/ en el repo: {data_dir}")

    refs = _index_tables(data_dir)
    if not refs:
        raise FileNotFoundError(f"No se detectaron tablas legibles en data/. Archivos: {[p.name for p in _list_data_files(data_dir)]}")

    ref_apt = _pick_table(refs, "apt_almacen")
    ref_zon = _pick_table(refs, "zonas")
    ref_caf = _pick_table(refs, "cafe")
    ref_thr = _pick_table(refs, "thresholds")

    missing = [k for k, r in (("apt_almacen", ref_apt), ("zonas", ref_zon), ("cafe", ref_caf), ("thresholds", ref_thr)) if r is None]
    if missing:
        detected = sorted({r.path.name for r in refs})
        raise ValueError(f"No pude detectar estos maestros en data/: {missing}. Archivos detectados: {detected}.")

    apt = _load_table(ref_apt)
    zonas = _load_table(ref_zon)
    cafe = _load_table(ref_caf)
    thresholds = _load_table(ref_thr)

    # ZONAS
    zonas = _rename_to_standard(zonas, {"apartamento": "APARTAMENTO", "zona": "ZONA"})
    if "APARTAMENTO" not in zonas.columns or "ZONA" not in zonas.columns:
        raise ValueError(f"Maestro zonas debe tener APARTAMENTO y ZONA. Columnas: {list(zonas.columns)}")
    zonas["APARTAMENTO"] = zonas["APARTAMENTO"].astype(str).str.strip()

    # CAFE
    cafe = _rename_to_standard(cafe, {"apartamento": "APARTAMENTO", "cafe_tipo": "CAFE_TIPO", "cafetipo": "CAFE_TIPO", "cafe": "CAFE_TIPO", "tipocafe": "CAFE_TIPO"})
    if "APARTAMENTO" not in cafe.columns or "CAFE_TIPO" not in cafe.columns:
        raise ValueError(f"Maestro cafe debe tener APARTAMENTO y CAFE_TIPO. Columnas: {list(cafe.columns)}")
    cafe["APARTAMENTO"] = cafe["APARTAMENTO"].astype(str).str.strip()

    # APT_ALMACEN (+ Localizacion)
    apt = _rename_to_standard(
        apt,
        {
            "apartamento": "APARTAMENTO",
            "almacen": "ALMACEN",
            "localizacion": "Localizacion",
            "localizaciongps": "Localizacion",
            "coordenadas": "Localizacion",
            "coords": "Localizacion",
            "gps": "Localizacion",
            "lat": "LAT",
            "lng": "LNG",
        },
    )
    if "Localización" in apt.columns and "Localizacion" not in apt.columns:
        apt = apt.rename(columns={"Localización": "Localizacion"})

    if "APARTAMENTO" not in apt.columns or "ALMACEN" not in apt.columns:
        raise ValueError(f"Maestro apt_almacen debe tener APARTAMENTO y ALMACEN. Columnas: {list(apt.columns)}")

    # Si no viene Localizacion, la creamos para que la app no pete
    if "Localizacion" not in apt.columns:
        apt["Localizacion"] = pd.NA

    apt["APARTAMENTO"] = apt["APARTAMENTO"].astype(str).str.strip()
    apt["ALMACEN"] = apt["ALMACEN"].astype(str).str.strip()

    # THRESHOLDS (solo limpiamos headers)
    thresholds.columns = _safe_columns(thresholds)
    if "AMENITY" in thresholds.columns and "Amenity" not in thresholds.columns:
        thresholds = thresholds.rename(columns={"AMENITY": "Amenity"})

    return {
        "zonas": zonas,
        "apt_almacen": apt,
        "cafe": cafe,
        "thresholds": thresholds,
    }
