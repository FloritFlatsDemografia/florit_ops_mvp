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


_COORD_RX = re.compile(r"^\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*$")


def _split_coord_to_lat_lng(coord_val):
    """
    Convierte "39.47,-0.37" -> (39.47, -0.37)
    Devuelve (None, None) si no puede.
    """
    if coord_val is None:
        return None, None
    s = str(coord_val).strip()
    if not s or s.lower() in ["nan", "none"]:
        return None, None

    m = _COORD_RX.match(s)
    if not m:
        return None, None
    try:
        lat = float(m.group(1))
        lng = float(m.group(2))
        return lat, lng
    except Exception:
        return None, None


def _ensure_apt_almacen(df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera poder obtener:
      - APARTAMENTO
      - ALMACEN (o equivalente)
      - Coordenadas en columna C (3ª) o en alguna columna tipo COORD/Coord/Coordenadas
    Devuelve df con columnas: APARTAMENTO, ALMACEN, LAT, LNG
    """
    df = _clean_cols(df)

    # Detecta columna apartamento
    apt_col = None
    for c in df.columns:
        cn = c.strip().lower()
        if cn in ["apartamento", "apartment", "alojamiento", "nombre", "propiedad"]:
            apt_col = c
            break
    if apt_col is None:
        # fallback: primera columna
        apt_col = df.columns[0]

    # Detecta columna almacén
    alm_col = None
    for c in df.columns:
        cn = c.strip().lower()
        if cn in ["almacen", "almacén", "ubicacion", "ubicación", "location", "almacen/ubicacion"]:
            alm_col = c
            break
    if alm_col is None:
        # fallback: intenta segunda columna
        alm_col = df.columns[1] if len(df.columns) > 1 else None

    # Detecta columna coordenadas:
    # 1) por nombre (coord/coordenadas/latlng)
    coord_col = None
    for c in df.columns:
        cn = c.strip().lower()
        if "coord" in cn or "coorden" in cn or "lat" in cn and "lng" in cn:
            coord_col = c
            break

    # 2) si no hay nombre, usa la columna C (3ª) como dices
    if coord_col is None and len(df.columns) >= 3:
        coord_col = df.columns[2]  # columna C

    out = pd.DataFrame()
    out["APARTAMENTO"] = df[apt_col].map(_safe_str)
    out["ALMACEN"] = df[alm_col].map(_safe_str) if alm_col else ""

    # Si ya existen LAT/LNG, respétalas; si no, créalas desde coord_col
    lat_col = None
    lng_col = None
    for c in df.columns:
        cn = c.strip().lower()
        if cn in ["lat", "latitude"]:
            lat_col = c
        if cn in ["lng", "lon", "long", "longitude"]:
            lng_col = c

    if lat_col and lng_col:
        out["LAT"] = pd.to_numeric(df[lat_col], errors="coerce")
        out["LNG"] = pd.to_numeric(df[lng_col], errors="coerce")
    else:
        # parsea "lat,lng" desde columna C (o detectada)
        lats = []
        lngs = []
        for v in df[coord_col] if coord_col in df.columns else []:
            lat, lng = _split_coord_to_lat_lng(v)
            lats.append(lat)
            lngs.append(lng)
        out["LAT"] = pd.to_numeric(pd.Series(lats), errors="coerce")
        out["LNG"] = pd.to_numeric(pd.Series(lngs), errors="coerce")

    out = out.dropna(subset=["APARTAMENTO", "ALMACEN"]).copy()
    out = out[out["APARTAMENTO"].astype(str).str.strip().ne("")].copy()
    out = out[out["ALMACEN"].astype(str).str.strip().ne("")].copy()

    # No obligamos a tener coords, pero si están, que sean numéricas
    out["LAT"] = pd.to_numeric(out["LAT"], errors="coerce")
    out["LNG"] = pd.to_numeric(out["LNG"], errors="coerce")

    return out.drop_duplicates(subset=["APARTAMENTO", "ALMACEN"])


@st.cache_data(show_spinner=False)
def load_masters_repo() -> dict:
    """
    Carga maestros desde data/ dentro del repo.
    Devuelve dict con keys:
      - zonas
      - apt_almacen   (incluye LAT/LNG ya parseado)
      - cafe
      - thresholds
    """
    masters = {}

    # ZONAS
    p = os.path.join(DATA_DIR, FILES["zonas"])
    zonas = _read_excel(p)
    zonas = _clean_cols(zonas)
    # Normaliza nombres esperados
    # Debe existir: APARTAMENTO, ZONA
    if "APARTAMENTO" not in zonas.columns:
        # intenta detectar
        for c in zonas.columns:
            if str(c).strip().lower() in ["apartamento", "alojamiento"]:
                zonas = zonas.rename(columns={c: "APARTAMENTO"})
                break
    if "ZONA" not in zonas.columns:
        for c in zonas.columns:
            if str(c).strip().lower() == "zona":
                zonas = zonas.rename(columns={c: "ZONA"})
                break
    zonas["APARTAMENTO"] = zonas["APARTAMENTO"].map(_safe_str)
    zonas["ZONA"] = zonas["ZONA"].map(_safe_str)
    masters["zonas"] = zonas[["APARTAMENTO", "ZONA"]].dropna(subset=["APARTAMENTO"]).drop_duplicates()

    # APARTAMENTOS + INVENTARIOS (ALMACEN + COORDS)
    p = os.path.join(DATA_DIR, FILES["apt_almacen"])
    apt = _read_excel(p)
    apt = _ensure_apt_almacen(apt)
    masters["apt_almacen"] = apt

    # CAFE
    p = os.path.join(DATA_DIR, FILES["cafe"])
    cafe = _read_excel(p)
    cafe = _clean_cols(cafe)
    if "APARTAMENTO" not in cafe.columns:
        for c in cafe.columns:
            if str(c).strip().lower() in ["apartamento", "alojamiento"]:
                cafe = cafe.rename(columns={c: "APARTAMENTO"})
                break
    if "CAFE_TIPO" not in cafe.columns:
        for c in cafe.columns:
            if "cafe" in str(c).strip().lower():
                cafe = cafe.rename(columns={c: "CAFE_TIPO"})
                break
    cafe["APARTAMENTO"] = cafe["APARTAMENTO"].map(_safe_str)
    cafe["CAFE_TIPO"] = cafe["CAFE_TIPO"].map(_safe_str)
    masters["cafe"] = cafe[["APARTAMENTO", "CAFE_TIPO"]].dropna(subset=["APARTAMENTO"]).drop_duplicates()

    # THRESHOLDS
    p = os.path.join(DATA_DIR, FILES["thresholds"])
    thr = _read_excel(p)
    thr = _clean_cols(thr)
    masters["thresholds"] = thr

    return masters
