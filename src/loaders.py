# src/loaders.py
import pandas as pd
import streamlit as st
import os
import re


DATA_DIR = "data"

FILES = {
    "zonas": "Agrupacion apartamentos por zona.xlsx",
    "apt_almacen": "Apartamentos e inventarios.xlsx",
    "cafe": "Cafe por apartamento.xlsx",
    "thresholds": "Stock minimo por almacen.xlsx",
}


def _read_excel(path: str) -> pd.DataFrame:
    return pd.read_excel(path)


def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _safe_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in ["nan", "none"]:
        return ""
    return s


# =========================
# Detectores flexibles
# =========================
def _find_col_by_keywords(df: pd.DataFrame, keywords: list[str]) -> str | None:
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    norm = {c: str(c).strip().lower() for c in cols}
    for c, n in norm.items():
        for k in keywords:
            if k in n:
                return c
    return None


# =========================
# Coordenadas (columna C "lat,lng")
# =========================
_COORD_RX = re.compile(r"^\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*$")


def _split_coord_to_lat_lng(coord_val):
    if coord_val is None:
        return None, None
    s = str(coord_val).strip()
    if not s or s.lower() in ["nan", "none"]:
        return None, None

    m = _COORD_RX.match(s)
    if not m:
        return None, None
    try:
        return float(m.group(1)), float(m.group(2))
    except Exception:
        return None, None


def _ensure_apt_almacen(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve df con columnas: APARTAMENTO, ALMACEN, LAT, LNG
    - Coordenadas: si no hay LAT/LNG, las lee de columna C (3ª) como "lat,lng"
    """
    df = _clean_cols(df)

    # Detecta APARTAMENTO
    apt_col = _find_col_by_keywords(df, ["apart", "aloj", "prop", "vivi", "nombre"])
    if apt_col is None:
        apt_col = df.columns[0]

    # Detecta ALMACEN/UBICACION
    alm_col = _find_col_by_keywords(df, ["almac", "ubic", "location"])
    if alm_col is None and len(df.columns) >= 2:
        alm_col = df.columns[1]

    # Detecta LAT/LNG si existen
    lat_col = _find_col_by_keywords(df, ["lat"])
    lng_col = _find_col_by_keywords(df, ["lng", "lon", "long"])

    # Detecta columna coordenadas por nombre, o usa columna C
    coord_col = _find_col_by_keywords(df, ["coord", "coorden"])
    if coord_col is None and len(df.columns) >= 3:
        coord_col = df.columns[2]  # columna C

    out = pd.DataFrame()
    out["APARTAMENTO"] = df[apt_col].map(_safe_str)
    out["ALMACEN"] = df[alm_col].map(_safe_str) if alm_col in df.columns else ""

    if lat_col in df.columns and lng_col in df.columns:
        out["LAT"] = pd.to_numeric(df[lat_col], errors="coerce")
        out["LNG"] = pd.to_numeric(df[lng_col], errors="coerce")
    else:
        lats, lngs = [], []
        if coord_col in df.columns:
            for v in df[coord_col]:
                lat, lng = _split_coord_to_lat_lng(v)
                lats.append(lat)
                lngs.append(lng)
        out["LAT"] = pd.to_numeric(pd.Series(lats), errors="coerce")
        out["LNG"] = pd.to_numeric(pd.Series(lngs), errors="coerce")

    out = out.dropna(subset=["APARTAMENTO"]).copy()
    out = out[out["APARTAMENTO"].astype(str).str.strip().ne("")].copy()

    out["ALMACEN"] = out["ALMACEN"].fillna("").astype(str).str.strip()
    out["LAT"] = pd.to_numeric(out["LAT"], errors="coerce")
    out["LNG"] = pd.to_numeric(out["LNG"], errors="coerce")

    return out.drop_duplicates(subset=["APARTAMENTO", "ALMACEN"])


def _ensure_zonas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve df con columnas: APARTAMENTO, ZONA
    Ultra-flexible:
    - intenta detectar por nombre
    - fallback: col0=APARTAMENTO, col1=ZONA
    """
    df = _clean_cols(df)

    apt_col = _find_col_by_keywords(df, ["apart", "aloj", "prop", "vivi", "nombre"])
    zona_col = _find_col_by_keywords(df, ["zona", "zone", "area", "barrio"])

    if apt_col is None:
        apt_col = df.columns[0]
    if zona_col is None:
        zona_col = df.columns[1] if len(df.columns) >= 2 else df.columns[0]

    out = pd.DataFrame()
    out["APARTAMENTO"] = df[apt_col].map(_safe_str)
    out["ZONA"] = df[zona_col].map(_safe_str)

    out = out.dropna(subset=["APARTAMENTO"]).copy()
    out = out[out["APARTAMENTO"].astype(str).str.strip().ne("")].copy()

    return out[["APARTAMENTO", "ZONA"]].drop_duplicates()


def _ensure_cafe(df: pd.DataFrame) -> pd.DataFrame:
    df = _clean_cols(df)

    apt_col = _find_col_by_keywords(df, ["apart", "aloj", "prop", "vivi", "nombre"])
    cafe_col = _find_col_by_keywords(df, ["cafe", "caf"])

    if apt_col is None:
        apt_col = df.columns[0]
    if cafe_col is None:
        cafe_col = df.columns[1] if len(df.columns) >= 2 else df.columns[0]

    out = pd.DataFrame()
    out["APARTAMENTO"] = df[apt_col].map(_safe_str)
    out["CAFE_TIPO"] = df[cafe_col].map(_safe_str)

    out = out.dropna(subset=["APARTAMENTO"]).copy()
    out = out[out["APARTAMENTO"].astype(str).str.strip().ne("")].copy()

    return out[["APARTAMENTO", "CAFE_TIPO"]].drop_duplicates()


@st.cache_data(show_spinner=False)
def load_masters_repo() -> dict:
    """
    Carga maestros desde data/ dentro del repo.
    Devuelve dict con keys:
      - zonas (APARTAMENTO, ZONA)
      - apt_almacen (APARTAMENTO, ALMACEN, LAT, LNG)
      - cafe (APARTAMENTO, CAFE_TIPO)
      - thresholds (tal cual)
    """
    masters = {}

    # ZONAS
    p = os.path.join(DATA_DIR, FILES["zonas"])
    zonas_raw = _read_excel(p)
    masters["zonas"] = _ensure_zonas(zonas_raw)

    # APARTAMENTOS + INVENTARIOS (ALMACEN + COORDS)
    p = os.path.join(DATA_DIR, FILES["apt_almacen"])
    apt_raw = _read_excel(p)
    masters["apt_almacen"] = _ensure_apt_almacen(apt_raw)

    # CAFE
    p = os.path.join(DATA_DIR, FILES["cafe"])
    cafe_raw = _read_excel(p)
    masters["cafe"] = _ensure_cafe(cafe_raw)

    # THRESHOLDS
    p = os.path.join(DATA_DIR, FILES["thresholds"])
    thr = _read_excel(p)
    masters["thresholds"] = _clean_cols(thr)

    return masters
