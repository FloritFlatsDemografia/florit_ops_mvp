import pandas as pd
from io import BytesIO, StringIO
import re
import csv


REQUIRED_AVANTIO_COLS = ["Alojamiento", "Fecha entrada hora", "Fecha salida hora"]


def _is_html_bytes(b: bytes) -> bool:
    head = b[:2000].lower()
    return (b"<html" in head) or (b"rec-html40" in head) or (b"<table" in head)


def _clean_avantio_html_tables(tables: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Avantio exporta .xls que en realidad es HTML con muchas tablas.
    Cada tabla suele tener:
      fila 0: "Lunes, 2 Febrero 2026"
      fila 1: cabecera real ("ID Reserva", "Localizador", "Alojamiento", ...)
      filas 2..n: reservas
    """
    frames = []

    for t in tables:
        if t is None or t.empty:
            continue

        t = t.dropna(how="all")
        if t.empty:
            continue

        header_idx = None
        for i in range(min(len(t), 15)):
            row = t.iloc[i].astype(str).str.strip().tolist()
            if any(x == "ID Reserva" for x in row):
                header_idx = i
                break

        if header_idx is None:
            continue

        header = t.iloc[header_idx].astype(str).str.strip().tolist()
        data = t.iloc[header_idx + 1:].copy()

        if data.shape[1] != len(header):
            n = min(data.shape[1], len(header))
            data = data.iloc[:, :n]
            header = header[:n]

        data.columns = header

        if "ID Reserva" in data.columns:
            data = data[data["ID Reserva"].astype(str).str.strip().ne("ID Reserva")]

        if "Alojamiento" in data.columns:
            data = data[data["Alojamiento"].notna()]

        if not data.empty:
            frames.append(data)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
            df[c] = df[c].replace({"nan": None, "None": None, "": None})

    return df


def _normalize_column_name(name: str) -> str:
    if name is None:
        return ""
    name = str(name).replace("\ufeff", "").strip()
    name = re.sub(r"\s+", " ", name)
    return name


def _try_read_csv_with_options(text: str, encoding_used: str) -> pd.DataFrame | None:
    """
    Intenta leer CSV de forma robusta:
    - autodetección de separador
    - fallback a separadores frecuentes
    - tolerancia a filas defectuosas
    """
    candidates = []

    try:
        sample = "\n".join(text.splitlines()[:20])
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        candidates.append(dialect.delimiter)
    except Exception:
        pass

    for sep in [";", ",", "\t", "|"]:
        if sep not in candidates:
            candidates.append(sep)

    tried = []

    for sep in candidates:
        try:
            df = pd.read_csv(
                StringIO(text),
                sep=sep,
                engine="python",
                on_bad_lines="skip",
            )

            df.columns = [_normalize_column_name(c) for c in df.columns]

            # descartamos lecturas absurdas de una sola columna cuando claramente no toca
            if df is not None and not df.empty and len(df.columns) >= 2:
                return df

            tried.append(f"sep={repr(sep)} -> {len(df.columns) if df is not None else 0} cols")
        except Exception as e:
            tried.append(f"sep={repr(sep)} -> {type(e).__name__}: {e}")

    return None


def _read_csv_robust(b: bytes) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

    errors = []

    for enc in encodings:
        try:
            text = b.decode(enc)
        except UnicodeDecodeError as e:
            errors.append(f"encoding={enc} -> UnicodeDecodeError: {e}")
            continue

        df = _try_read_csv_with_options(text, enc)
        if df is not None and not df.empty:
            return df

        errors.append(f"encoding={enc} -> no se obtuvo una tabla válida")

    raise ValueError(
        "Avantio: no se pudo interpretar el CSV. "
        "Posible causa: separador no estándar, codificación distinta o filas rotas."
    )


def parse_avantio_entradas(uploaded_file) -> pd.DataFrame:
    """
    Acepta:
      - .xls HTML (Avantio típico)
      - .xlsx real
      - .csv
    Devuelve DataFrame con las columnas originales de Avantio
    (incluyendo REQUIRED_AVANTIO_COLS).
    """
    b = uploaded_file.getvalue() if hasattr(uploaded_file, "getvalue") else uploaded_file.read()
    name = getattr(uploaded_file, "name", "") or ""
    lower_name = name.lower()

    # 1) CSV
    if lower_name.endswith(".csv"):
        df = _read_csv_robust(b)

    # 2) HTML disfrazado de XLS
    elif _is_html_bytes(b):
        try:
            tables = pd.read_html(BytesIO(b))
            df = _clean_avantio_html_tables(tables)
        except Exception as e:
            raise ValueError(f"Avantio: error leyendo .xls HTML: {e}") from e

    # 3) Excel real
    else:
        try:
            df = pd.read_excel(BytesIO(b))
        except Exception as e:
            raise ValueError(f"Avantio: error leyendo Excel: {e}") from e

    if df is None or df.empty:
        raise ValueError("Avantio: no se han podido leer datos (archivo vacío o formato no soportado).")

    # Normalizamos nombres de columnas
    df.columns = [_normalize_column_name(c) for c in df.columns]

    # A veces vienen columnas con espacios invisibles o variantes extrañas
    # Intento de renombrado flexible para las tres obligatorias
    rename_map = {}
    for c in df.columns:
        lc = c.lower()

        if lc == "alojamiento":
            rename_map[c] = "Alojamiento"
        elif lc in ["fecha entrada hora", "fecha entrada", "entrada hora", "check in", "check-in"]:
            rename_map[c] = "Fecha entrada hora"
        elif lc in ["fecha salida hora", "fecha salida", "salida hora", "check out", "check-out"]:
            rename_map[c] = "Fecha salida hora"

    if rename_map:
        df = df.rename(columns=rename_map)

    detected = list(df.columns)
    missing = [c for c in REQUIRED_AVANTIO_COLS if c not in df.columns]

    if missing:
        raise ValueError(
            f"Avantio: faltan columnas requeridas: {missing}. Detectadas={detected[:50]}"
        )

    # Convertimos fechas
    df["Fecha entrada hora"] = pd.to_datetime(
        df["Fecha entrada hora"],
        errors="coerce",
        dayfirst=True
    )
    df["Fecha salida hora"] = pd.to_datetime(
        df["Fecha salida hora"],
        errors="coerce",
        dayfirst=True
    )

    # Limpieza alojamiento
    df["Alojamiento"] = df["Alojamiento"].astype(str).str.strip()

    # Eliminamos filas totalmente inútiles
    df = df.dropna(how="all")

    # Si una fila no tiene alojamiento ni fechas, fuera
    df = df[
        ~(
            df["Alojamiento"].replace({"": None, "nan": None}).isna()
            & df["Fecha entrada hora"].isna()
            & df["Fecha salida hora"].isna()
        )
    ].copy()

    if df.empty:
        raise ValueError("Avantio: tras limpiar el archivo no quedan filas válidas.")

    return df


def parse_odoo_stock(uploaded_file) -> pd.DataFrame:
    """
    Espera:
      - xlsx/csv con columnas al menos: 'Ubicación' y 'Producto' y 'Cantidad' (o similar)
    Si tu export de Odoo tiene nombres distintos, aquí se adapta.
    """
    b = uploaded_file.getvalue() if hasattr(uploaded_file, "getvalue") else uploaded_file.read()
    name = getattr(uploaded_file, "name", "") or ""

    if name.lower().endswith(".csv"):
        # también lo hacemos más robusto
        try:
            df = _read_csv_robust(b)
        except Exception:
            df = pd.read_csv(BytesIO(b))
    else:
        df = pd.read_excel(BytesIO(b))

    if df is None or df.empty:
        return pd.DataFrame()

    df.columns = [_normalize_column_name(c) for c in df.columns]

    col_ubic = None
    for c in df.columns:
        if c.lower() in ["ubicación", "ubicacion", "location", "ubicacion/stock"]:
            col_ubic = c
            break
        if "ubic" in c.lower():
            col_ubic = c
            break

    col_prod = None
    for c in df.columns:
        if c.lower() in ["producto", "product", "nombre producto", "product name"]:
            col_prod = c
            break
        if "product" in c.lower() or "producto" in c.lower():
            col_prod = c
            break

    col_qty = None
    for c in df.columns:
        if c.lower() in ["cantidad", "quantity", "qty", "on hand", "disponible"]:
            col_qty = c
            break
        if "cant" in c.lower() or "quant" in c.lower() or "qty" in c.lower():
            col_qty = c
            break

    if not (col_ubic and col_prod and col_qty):
        raise ValueError(
            f"Odoo: no se detectan columnas. Encontradas={list(df.columns)} | "
            f"Detectadas: ubic={col_ubic}, prod={col_prod}, qty={col_qty}"
        )

    out = df[[col_ubic, col_prod, col_qty]].copy()
    out.columns = ["Ubicación", "Producto", "Cantidad"]

    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0)
    out["Ubicación"] = out["Ubicación"].astype(str).str.strip()
    out["Producto"] = out["Producto"].astype(str).str.strip()

    return out
