from __future__ import annotations

import streamlit as st
import pandas as pd

def read_sheet_df() -> pd.DataFrame:
    """
    Lee un Google Sheet con gspread usando credenciales en st.secrets.

    Secrets requeridos:
      gsheet_url = "https://docs.google.com/spreadsheets/d/....../edit"
      gsheet_tab = "Respuestas de formulario 1"
      [gcp_service_account] ... (service account JSON en TOML)
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception as e:
        raise RuntimeError("Faltan dependencias: instala gspread y google-auth en requirements.txt") from e

    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("No existe [gcp_service_account] en Secrets de Streamlit.")

    gsheet_url = st.secrets.get("gsheet_url", "")
    gsheet_tab = st.secrets.get("gsheet_tab", "")

    if not gsheet_url:
        raise RuntimeError("Falta gsheet_url en Secrets.")
    if not gsheet_tab:
        raise RuntimeError("Falta gsheet_tab en Secrets.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_url(gsheet_url)
    ws = sh.worksheet(gsheet_tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return pd.DataFrame()

    headers = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=headers)
    return df
