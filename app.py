# src/loaders.py
from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from functools import lru_cache
import pandas as pd


# =========================================
# Helpers de normalización / detección
# =========================================
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


def _read_csv(path: Path) -> pd.DataFrame:
    # Intentos típicos para CSVs
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    # último intento
    return pd.read_csv(path, encoding="latin-1", errors="ignore")


@dataclass
class TableRef:
    path: Path
    kind: str  # "excel" o "csv"
    sheet: str | None
    cols_norm: set[str]


def _list_data_files(data_dir: Path) -> list[Path]:
    exts = (".xlsx", ".xls", ".csv")
    files = []
    for p in data_dir.glob("*"):
        if p.is_file() and p.suffix.lower() in exts:
            files.append(p)
    return sorted(files)


def _index_tables(data_dir: Path) -> list[TableRef]:
    """
    Indexa todas las tablas en /data:
      - CSV: una tabla
      - Excel: una tabla por hoja
    Lee SOLO headers (nrows=0) para detectar columnas.
    """
    refs: list[TableRef] = []
    for path in _list_data_files(data_dir):
        suf = path.suffix.lower()
        if suf == ".csv":
            try:
                df0 = _read_csv(path)
                cols = {_norm_col(c) for c in _safe_columns(df0)}
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
    """
    Scoring heurístico para elegir la mejor tabla para cada maestro.
    """
    fn = ref.path.name.lower()
    sh = (ref.sheet or "").lower()

    has_ap = "apartamento" in ref.cols_norm
    has_al = "almacen" in ref.cols_norm
    has_z = "zona" in ref.cols_norm
    has_cafe = any(k in ref.cols_norm for k in ("cafetipo", "cafe", "tipo_cafe", "cafe_tipo"))
    has_amenity = "amenity" in ref.cols_norm

    # min/max típicos (thresholds)
    has_min = any(k in ref.cols_norm for k in ("min", "minimo", "stockmin", "stockminimo"))
    has_max = any(k in ref.cols_norm for k in ("max", "maximo", "stockmax", "stockmaximo"))

    # localizacion/coords
    has_loc = any(k in ref.cols_norm for k in ("localizacion", "localizaciongps", "gps", "coords", "coordenadas", "lat", "lng"))

    score = 0

    if want == "apt_almacen":
        if has_ap and has_al:
            score += 100
        if has_loc:
            score += 25
        if "apart" in fn or "invent" in fn or "almacen" in fn:
            score += 10
        if "almacen" in sh or "apto" in sh or "apart" in sh:
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
        # exigimos Amenity y algo tipo min/max
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
    if best_score < 80:  # umbral: evita elegir “cualquier cosa”
        return None
    return best


def _load_table(ref: TableRef) -> pd.DataFrame:
    if ref.kind == "csv":
        df = _read_csv(ref.path)
    else:
        df = pd.read_excel(ref.path, sheet_name=ref.sheet)
    df.columns = _safe_columns(df)
    return df


def _rename_to_standard(df: pd.DataFrame, desired: dict[str, str]) -> pd.DataFrame:
    """
    desired: norm_key -> standard_name
    """
    cols = list(df.columns)
    norm_map = {_norm_col(c): c for c in cols}
    rename = {}
    for nk, std in desired.items():
        if nk in norm_map:
            rename[norm_map[nk]] = std
    return df.rename(columns=rename)


# =========================================
# API pública (mantiene tu interfaz actual)
# =========================================
@lru_cache(maxsize=1)
def load_masters_repo() -> dict:
    """
    Carga maestros desde /data del repo.
    Devuelve dict con keys:
      - zonas
      - apt_almacen
      - cafe
      - thresholds
    """
    base_dir = Path(__file__).resolve().parents[1]
    data_dir = base_dir / "data"
    if not data_dir.exists():
        raise FileNotFoundError(f"No existe carpeta data/ en el repo: {data_dir}")

    refs = _index_tables(data_dir)

    if not refs:
        files = [p.name for p in _list_data_files(data_dir)]
        raise FileNotFoundError(f"No se detectaron tablas legibles en data/. Archivos: {files}")

    # Elegimos tablas
    ref_apt = _pick_table(refs, "apt_almacen")
    ref_zon = _pick_table(refs, "zonas")
    ref_caf = _pick_table(refs, "cafe")
    ref_thr = _pick_table(refs, "thresholds")

    missing = []
    if ref_apt is None: missing.append("apt_almacen")
    if ref_zon is None: missing.append("zonas")
    if ref_caf is None: missing.append("cafe")
    if ref_thr is None: missing.append("thresholds")

    if missing:
        # Mensaje “accionable”
        detected = sorted({r.path.name for r in refs})
        raise ValueError(
            "No pude detectar estos maestros en data/: "
            f"{missing}. Archivos detectados: {detected}. "
            "Asegura que existan hojas/CSVs con columnas esperadas."
        )

    # Cargar DF
    apt = _load_table(ref_apt)
    zonas = _load_table(ref_zon)
    cafe = _load_table(ref_caf)
    thresholds = _load_table(ref_thr)

    # Normalizar columnas mínimas esperadas
    # ZONAS: APARTAMENTO, ZONA
    zonas = _rename_to_standard(zonas, {"apartamento": "APARTAMENTO", "zona": "ZONA"})
    if "APARTAMENTO" not in zonas.columns:
        raise ValueError(f"Maestro zonas sin APARTAMENTO. Columnas: {list(zonas.columns)}")
    if "ZONA" not in zonas.columns:
        # si existe algo parecido, mejor no inventar: que falle claro
        raise ValueError(f"Maestro zonas sin ZONA. Columnas: {list(zonas.columns)}")
    zonas["APARTAMENTO"] = zonas["APARTAMENTO"].astype(str).str.strip()

    # CAFE: APARTAMENTO, CAFE_TIPO
    cafe = _rename_to_standard(cafe, {"apartamento": "APARTAMENTO", "cafetipo": "CAFE_TIPO", "cafe_tipo": "CAFE_TIPO", "cafe": "CAFE_TIPO"})
    if "APARTAMENTO" not in cafe.columns or "CAFE_TIPO" not in cafe.columns:
        raise ValueError(f"Maestro cafe debe tener APARTAMENTO y CAFE_TIPO. Columnas: {list(cafe.columns)}")
    cafe["APARTAMENTO"] = cafe["APARTAMENTO"].astype(str).str.strip()

    # APT_ALMACEN: APARTAMENTO, ALMACEN, Localizacion (opcional pero la forzamos)
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
            "localizacion ": "Localizacion",
            "localizacion\t": "Localizacion",
        },
    )

    # Si venía con acento:
    if "Localización" in apt.columns and "Localizacion" not in apt.columns:
        apt = apt.rename(columns={"Localización": "Localizacion"})

    if "APARTAMENTO" not in apt.columns or "ALMACEN" not in apt.columns:
        raise ValueError(f"Maestro apt_almacen debe tener APARTAMENTO y ALMACEN. Columnas: {list(apt.columns)}")

    if "Localizacion" not in apt.columns:
        # clave: si no existe, la creamos para que la app no falle
        apt["Localizacion"] = pd.NA

    apt["APARTAMENTO"] = apt["APARTAMENTO"].astype(str).str.strip()
    apt["ALMACEN"] = apt["ALMACEN"].astype(str).str.strip()

    # THRESHOLDS: no renombro agresivo para no romper tu lógica interna,
    # solo limpio headers y aseguro "Amenity" si viniera en mayúsculas.
    thresholds.columns = _safe_columns(thresholds)
    # si existiera AMENITY -> Amenity
    if "AMENITY" in thresholds.columns and "Amenity" not in thresholds.columns:
        thresholds = thresholds.rename(columns={"AMENITY": "Amenity"})

    return {
        "zonas": zonas,
        "apt_almacen": apt,
        "cafe": cafe,
        "thresholds": thresholds,
    }
