# src/parsers/cleaning_last_report.py
import re
from datetime import datetime
import pandas as pd


def _normalize_apt(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()

    # Normalización rápida (acentos básicos + espacios)
    repl = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n",
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ñ": "N",
    }
    for a, b in repl.items():
        s = s.replace(a, b)

    s = re.sub(r"\s+", " ", s)

    # Quita ceros iniciales en números sueltos: "APOLO 029" -> "APOLO 29"
    s = re.sub(r"\b0+(\d)", r"\1", s)

    return s.upper().strip()


def _parse_timestamp(x):
    if pd.isna(x):
        return pd.NaT
    if isinstance(x, datetime):
        return x
    s = str(x).strip()

    # Formatos habituales de Google Forms
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

    return pd.to_datetime(s, errors="coerce", dayfirst=True)


def _find_col(df: pd.DataFrame, exact: str, fallback_pattern: str | None = None) -> str | None:
    # 1) exact match (case-insensitive)
    for c in df.columns:
        if str(c).strip().lower() == exact.strip().lower():
            return c

    # 2) fallback regex
    if fallback_pattern:
        pat = re.compile(fallback_pattern, re.I)
        for c in df.columns:
            if pat.search(str(c)):
                return c

    return None


def build_last_report_view(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve 1 fila por apartamento: la última por Marca temporal,
    con LLAVES + OTRAS REPOSICIONES + INCIDENCIAS/TAREAS A REALIZAR.
    """

    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "Apartamento", "Último informe", "LLAVES", "OTRAS REPOSICIONES", "INCIDENCIAS/TAREAS A REALIZAR",
            "flag_llaves", "flag_otras_repos", "flag_incidencias"
        ])

    col_ts = _find_col(df, "Marca temporal", r"^marca\s*temporal$")
    col_apt = _find_col(df, "Apartamento", r"^apartamento$")
    col_alt = _find_col(df, "Si es otro piso indicar aqui", r"otro\s*piso|indicar\s*aqui")

    col_llaves = _find_col(df, "LLAVES", r"^llaves$")
    col_otras = _find_col(df, "OTRAS REPOSICIONES", r"otras\s*reposiciones")
    col_incid = _find_col(df, "INCIDENCIAS/TAREAS A REALIZAR", r"incidencias|tareas\s*a\s*realizar")

    missing = [name for name, c in [
        ("Marca temporal", col_ts),
        ("Apartamento", col_apt),
        ("LLAVES", col_llaves),
        ("OTRAS REPOSICIONES", col_otras),
        ("INCIDENCIAS/TAREAS A REALIZAR", col_incid),
    ] if c is None]

    if missing:
        raise KeyError(f"Faltan columnas en la sheet (cabeceras): {missing}")

    tmp = df.copy()

    # Apartamento final (si es "Otro", usa el alternativo)
    apt = tmp[col_apt].astype(str).fillna("").str.strip()
    if col_alt:
        alt = tmp[col_alt].astype(str).fillna("").str.strip()
        apt_final = apt.where(~apt.str.lower().eq("otro"), alt)
    else:
        apt_final = apt

    tmp["_apt_norm"] = apt_final.map(_normalize_apt)
    tmp["_ts"] = tmp[col_ts].map(_parse_timestamp)

    tmp = tmp.dropna(subset=["_ts"])
    tmp = tmp.sort_values("_ts")

    last = tmp.groupby("_apt_norm", as_index=False).tail(1)

    out = last[["_apt_norm", "_ts", col_llaves, col_otras, col_incid]].copy()
    out = out.rename(columns={
        "_apt_norm": "Apartamento",
        "_ts": "Último informe",
        col_llaves: "LLAVES",
        col_otras: "OTRAS REPOSICIONES",
        col_incid: "INCIDENCIAS/TAREAS A REALIZAR",
    })

    def _has_text(v):
        t = "" if v is None else str(v).strip()
        if not t:
            return False
        t_low = t.lower()
        if t_low in {"n/a", "na", "-", "no es necesario"}:
            return False
        return True

    out["flag_llaves"] = out["LLAVES"].apply(_has_text)
    out["flag_otras_repos"] = out["OTRAS REPOSICIONES"].apply(_has_text)
    out["flag_incidencias"] = out["INCIDENCIAS/TAREAS A REALIZAR"].apply(_has_text)

    out = out.sort_values("Apartamento").reset_index(drop=True)
    return out

