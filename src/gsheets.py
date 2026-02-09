from __future__ import annotations

import pandas as pd
import streamlit as st


@st.cache_data(ttl=60, show_spinner=False)
def read_sheet_df() -> pd.DataFrame:
    """
    Lee un Google Sheet (worksheet) usando credenciales de Service Account en st.secrets.

    Requiere en Secrets:
      gsheet_url = "https://docs.google.com/spreadsheets/d/....../edit"
      gsheet_tab = "Respuestas de formulario 1"

      [gcp_service_account]
      type="service_account"
      ...
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception as e:
        raise RuntimeError(
            "Faltan dependencias. Añade a requirements.txt: gspread y google-auth"
        ) from e

    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("No existe [gcp_service_account] en Secrets de Streamlit.")

    gsheet_url = str(st.secrets.get("gsheet_url", "")).strip()
    gsheet_tab = str(st.secrets.get("gsheet_tab", "")).strip()

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
