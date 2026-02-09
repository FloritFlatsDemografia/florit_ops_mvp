# src/gsheets.py
from __future__ import annotations

import pandas as pd
import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo


def _safe_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in ("nan", "none"):
        return ""
    return s


def _tz():
    return ZoneInfo("Europe/Madrid")


def _parse_date_from_timestamp(x):
    """
    Columna A suele ser "Marca temporal" (timestamp).
    Convertimos a date en Europe/Madrid.
    """
    if x is None:
        return None
    try:
        ts = pd.to_datetime(x, errors="coerce")
        if pd.isna(ts):
            return None
        if ts.tzinfo is None:
            # si viene naive, asumimos Europe/Madrid
            ts = ts.tz_localize(_tz())
        else:
            ts = ts.tz_convert(_tz())
        return ts.date()
    except Exception:
        return None


@st.cache_data(show_spinner=False, ttl=60)  # refresca cada 60s
def load_cleaning_sheet_gspread() -> pd.DataFrame:
    """
    Lee Google Sheet usando Service Account guardado en st.secrets["gcp_service_account"].
    Requiere:
      - gspread
      - google-auth
    Secrets:
      - gsheet_url OR gsheet_id
      - gsheet_tab (opcional, por defecto primera hoja)
    """
    import gspread
    from google.oauth2.service_account import Credentials

    if "gcp_service_account" not in st.secrets:
        raise ValueError("Falta st.secrets['gcp_service_account'] (Service Account JSON).")

    sa_info = dict(st.secrets["gcp_service_account"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    gc = gspread.authorize(creds)

    gsheet_tab = st.secrets.get("gsheet_tab", None)

    if "gsheet_url" in st.secrets and st.secrets["gsheet_url"]:
        sh = gc.open_by_url(st.secrets["gsheet_url"])
    elif "gsheet_id" in st.secrets and st.secrets["gsheet_id"]:
        sh = gc.open_by_key(st.secrets["gsheet_id"])
    else:
        raise ValueError("Define st.secrets['gsheet_url'] o st.secrets['gsheet_id'].")

    ws = sh.worksheet(gsheet_tab) if gsheet_tab else sh.get_worksheet(0)
    values = ws.get_all_values()

    if not values or len(values) < 2:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def normalize_cleaning_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza columnas por letras/posición según tu definición:
      A: Marca temporal
      B: Apartamento
      G: Incidencias a realizar
      N: Inventario ropa e consumibles
      Q: Faltantes por entrada
      S..AR: reposiciones/consumibles (café, sal, té, detergente... finalizados)
    """
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    # Si viniera con headers raros, intentamos mapear por nombre; si no, por posición.
    # Preferimos por posición si el sheet es estable.
    cols = list(out.columns)

    def col_at(idx: int) -> str | None:
        return cols[idx] if idx < len(cols) else None

    # A, B, G, N, Q, S..AR
    c_ts = col_at(0)
    c_apt = col_at(1)
    c_incid = col_at(6)
    c_inv = col_at(13)
    c_falt = col_at(16)

    # Desde S (19) hasta AR (43) inclusive en notación Excel => índices 18..43 (0-based)
    # Pero ojo: Excel S es la 19ª columna => índice 18.
    # AR es la 44ª columna => índice 43.
    start_rep = 18
    end_rep = 43

    rep_cols = [cols[i] for i in range(start_rep, min(end_rep + 1, len(cols)))]

    norm = pd.DataFrame()
    norm["Marca temporal"] = out[c_ts] if c_ts else ""
    norm["Apartamento"] = out[c_apt] if c_apt else ""
    norm["Incidencias a realizar"] = out[c_incid] if c_incid else ""
    norm["Inventario ropa y consumibles"] = out[c_inv] if c_inv else ""
    norm["Faltantes por entrada"] = out[c_falt] if c_falt else ""

    # Añadimos todas las columnas de reposición tal cual
    for c in rep_cols:
        norm[c] = out[c]

    # Limpieza
    norm["Apartamento"] = norm["Apartamento"].map(_safe_str).str.upper()
    norm["Incidencias a realizar"] = norm["Incidencias a realizar"].map(_safe_str)
    norm["Inventario ropa y consumibles"] = norm["Inventario ropa y consumibles"].map(_safe_str)
    norm["Faltantes por entrada"] = norm["Faltantes por entrada"].map(_safe_str)

    # Fecha operativa desde timestamp
    norm["Fecha"] = norm["Marca temporal"].apply(_parse_date_from_timestamp)

    return norm


def latest_rows_for_date(df_norm: pd.DataFrame, target_date) -> pd.DataFrame:
    """
    Devuelve la última fila por Apartamento para una Fecha dada.
    """
    if df_norm is None or df_norm.empty:
        return pd.DataFrame()

    d = df_norm.copy()
    d = d[d["Fecha"] == target_date].copy()
    if d.empty:
        return d

    # Reconvertimos Marca temporal para ordenar
    d["_ts"] = pd.to_datetime(d["Marca temporal"], errors="coerce")
    d = d.sort_values("_ts").drop_duplicates("Apartamento", keep="last")
    d = d.drop(columns=["_ts"], errors="ignore")
    return d.reset_index(drop=True)
