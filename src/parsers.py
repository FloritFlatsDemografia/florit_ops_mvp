import pandas as pd
from io import BytesIO, StringIO
import re
import csv


REQUIRED_AVANTIO_COLS = ["Alojamiento", "Fecha entrada hora", "Fecha salida hora"]


def _is_html_bytes(b: bytes) -> bool:
    head = b[:2000].lower()
    return (b"<html" in head) or (b"rec-html40" in head) or (b"<table" in head)


def _normalize_column_name(name: str) -> str:
    if name is None:
        return ""
    name = str(name).replace("\ufeff", "").strip()
    name = re.sub(r"\s+", " ", name)
    return name


def _header_score(values: list[str]) -> int:
    normalized = [_normalize_column_name(v).lower() for v in values]
    score = 0

    wanted_exact = {
        "alojamiento",
        "fecha entrada hora",
        "fecha salida hora",
        "fecha entrada",
        "fecha salida",
        "id reserva",
        "localizador",
    }

    for v in normalized:
        if v in wanted_exact:
            score += 2

    joined = " | ".join(normalized)
    if "alojamiento" in joined:
        score += 2
    if "fecha entrada" in joined:
        score += 2
    if "fecha salida" in joined:
        score += 2

    return score


def _promote_header_row(df: pd.DataFrame, max_scan_rows: int = 30) -> pd.DataFrame:
    """
    Busca una fila interna que sea la cabecera real y la promueve.
    Funciona para CSV/Excel con títulos previos como 'Entradas'.
    """
    if df is None or df.empty:
        return df

    best_idx = None
    best_score = -1

    scan_limit = min(len(df), max_scan_rows)

    for i in range(scan_limit):
        row_vals = df.iloc[i].astype(str).fillna("").tolist()
        score = _header_score(row_vals)
        if score > best_score:
            best_score = score
            best_idx = i

    if best_idx is None or best_score < 3:
        return df

    new_header = [_normalize_column_name(v) for v in df.iloc[best_idx].tolist()]
    data = df.iloc[best_idx + 1:].copy().reset_index(drop=True)

    n = min(len(new_header), data.shape[1])
    data = data.iloc[:, :n]
    new_header = new_header[:n]

    clean_cols = []
    seen = {}

    for c in new_header:
        c = _normalize_column_name(c)
        if not c or c.lower().startswith("unnamed:"):
            c = ""

        base = c if c else "col"
        if base in seen:
            seen[base] += 1
            final = f"{base}_{seen[base]}"
        else:
            seen[base] = 0
            final = base

        clean_cols.append(final)

    data.columns = clean_cols
    return data


def _rename_avantio_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}

    for c in df.columns:
        lc = _normalize_column_name(c).lower()

        if lc == "alojamiento":
            rename_map[c] = "Alojamiento"
        elif lc in ["fecha entrada hora", "fecha entrada", "entrada hora", "check in", "check-in"]:
            rename_map[c] = "Fecha entrada hora"
        elif lc in ["fecha salida hora", "fecha salida", "salida hora", "check out", "check-out"]:
            rename_map[c] = "Fecha salida hora"

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _clean_avantio_html_tables(tables: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Avantio exporta .xls que en realidad es HTML con muchas tablas.
    """
    frames = []

    for t in tables:
        if t is None or t.empty:
            continue

        t = t.dropna(how="all")
        if t.empty:
            continue

        t = t.reset_index(drop=True)
        t = _promote_header_row(t, max_scan_rows=20)
        t.columns = [_normalize_column_name(c) for c in t.columns]
        t = _rename_avantio_columns(t)

        if "Alojamiento" not in t.columns:
            continue

        if "ID Reserva" in t.columns:
            t = t[t["ID Reserva"].astype(str).str.strip().ne("ID Reserva")]

        t = t[t["Alojamiento"].notna()]

        if not t.empty:
            frames.append(t)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
            df[c] = df[c].replace({"nan": None, "None": None, "": None})

    return df


def _try_read_csv_with_options(text: str) -> pd.DataFrame | None:
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

    for sep in candidates:
        try:
            # OJO: header=None es la clave
            raw = pd.read_csv(
                StringIO(text),
                sep=sep,
                engine="python",
                on_bad_lines="skip",
                header=None,
            )

            if raw is None or raw.empty:
                continue

            df = _promote_header_row(raw, max_scan_rows=30)
            df.columns = [_normalize_column_name(c) for c in df.columns]
            df = _rename_avantio_columns(df)

            if len(df.columns) >= 2:
                return df

        except Exception:
            continue

    return None


def _read_csv_robust(b: bytes) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

    for enc in encodings:
        try:
            text = b.decode(enc)
        except UnicodeDecodeError:
            continue

        df = _try_read_csv_with_options(text)
        if df is not None and not df.empty:
            return df

    raise ValueError(
        "Avantio: no se pudo interpretar el CSV. "
        "Posible causa: separador no estándar, codificación distinta o cabecera desplazada."
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

    if lower_name.endswith(".csv"):
        df = _read_csv_robust(b)

    elif _is_html_bytes(b):
        try:
            tables = pd.read_html(BytesIO(b), header=None)
            df = _clean_avantio_html_tables(tables)
        except Exception as e:
            raise ValueError(f"Avantio: error leyendo .xls HTML: {e}") from e

    else:
        try:
            # OJO: header=None es la clave también aquí
            raw = pd.read_excel(BytesIO(b), header=None)
            df = _promote_header_row(raw, max_scan_rows=30)
            df.columns = [_normalize_column_name(c) for c in df.columns]
            df = _rename_avantio_columns(df)
        except Exception as e:
            raise ValueError(f"Avantio: error leyendo Excel: {e}") from e

    if df is None or df.empty:
        raise ValueError("Avantio: no se han podido leer datos (archivo vacío o formato no soportado).")

    df.columns = [_normalize_column_name(c) for c in df.columns]
    df = _rename_avantio_columns(df)

    detected = list(df.columns)
    missing = [c for c in REQUIRED_AVANTIO_COLS if c not in df.columns]

    if missing:
        raise ValueError(
            f"Avantio: faltan columnas requeridas: {missing}. Detectadas={detected[:50]}"
        )

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

    df["Alojamiento"] = df["Alojamiento"].astype(str).str.strip()

    df = df.dropna(how="all")

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
        try:
            df = pd.read_csv(BytesIO(b))
        except Exception:
            df = _read_csv_robust(b)
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
