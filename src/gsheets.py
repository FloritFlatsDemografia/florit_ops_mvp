from __future__ import annotations

import pandas as pd
import streamlit as st


def _sanitize_private_key(pk: str) -> str:
    """
    Streamlit secrets a veces guarda el private_key con:
      - "\\n" literales en vez de saltos de línea reales
      - comillas envolviendo el texto
      - espacios / caracteres basura antes de '-----BEGIN'
    Esto lo deja como PEM válido.
    """
    if pk is None:
        return ""

    s = str(pk).strip()

    # Quita comillas envolventes si las hay
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # Si viene con \n literales, conviértelos a saltos reales
    if "\\n" in s and "\n" not in s:
        s = s.replace("\\n", "\n")

    # Recorta basura antes del BEGIN si existiera
    begin = "-----BEGIN PRIVATE KEY-----"
    if begin in s:
        s = begin + s.split(begin, 1)[1]

    # Asegura salto final (a veces ayuda)
    if s and not s.endswith("\n"):
        s += "\n"

    return s


@st.cache_data(ttl=60, show_spinner=False)
def read_sheet_df() -> pd.DataFrame:
    """
    Lee un Google Sheet (worksheet) usando Service Account en st.secrets.

    Requiere Secrets:
      gsheet_url = "https://docs.google.com/spreadsheets/d/....../edit"
      gsheet_tab = "Respuestas de formulario 1"

      [gcp_service_account]
      type="service_account"
      ...
      private_key="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception as e:
        raise RuntimeError("Faltan dependencias. Añade a requirements.txt: gspread y google-auth") from e

    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("No existe [gcp_service_account] en Secrets de Streamlit.")

    gsheet_url = str(st.secrets.get("gsheet_url", "")).strip()
    gsheet_tab = str(st.secrets.get("gsheet_tab", "")).strip()
    if not gsheet_url:
        raise RuntimeError("Falta gsheet_url en Secrets.")
    if not gsheet_tab:
        raise RuntimeError("Falta gsheet_tab en Secrets.")

    info = dict(st.secrets["gcp_service_account"])

    # ✅ Sanitiza private_key (causa típica de tu error)
    pk = info.get("private_key", "")
    info["private_key"] = _sanitize_private_key(pk)

    # Debug útil (sin exponer clave completa)
    if not info["private_key"].startswith("-----BEGIN PRIVATE KEY-----"):
        raise RuntimeError(
            "private_key inválida en Secrets: no empieza por '-----BEGIN PRIVATE KEY-----'. "
            "Revisa formato (saltos de línea / comillas / caracteres extra)."
        )

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_url(gsheet_url)
    ws = sh.worksheet(gsheet_tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return pd.DataFrame()

    headers = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=headers)
