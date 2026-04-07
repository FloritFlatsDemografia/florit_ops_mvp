import pandas as pd
from io import BytesIO, StringIO
import re
import csv


REQUIRED_AVANTIO_COLS = ["Alojamiento", "Fecha entrada hora", "Fecha salida hora"]


def _is_html_bytes(b: bytes) -> bool:
    head = b[:4000].lower()
    return (b"<html" in head) or (b"rec-html40" in head) or (b"<table" in head)


def _normalize_column_name(name) -> str:
    if name is None:
        return ""
    name = str(name).replace("\ufeff", "").strip()
    name = re.sub(r"\s+", " ", name)
    return name


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


def _header_score(values: list[str]) -> int:
    vals = [_normalize_column_name(v).lower() for v in values]
    score = 0

    for v in vals:
        if v == "alojamiento":
            score += 4
        elif v in ["fecha entrada hora", "fecha entrada"]:
            score += 4
        elif v in ["fecha salida hora", "fecha salida"]:
            score += 4
        elif v in ["id reserva", "localizador", "ocupante", "cliente"]:
            score += 1

    return score


def _promote_header_row(df: pd.DataFrame, max_scan_rows: int = 40) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    best_idx = None
    best_score = -1

    for i in range(min(len(df), max_scan_rows)):
        row_vals = df.iloc[i].astype(str).tolist()
        score = _header_score(row_vals)
        if score > best_score:
            best_score = score
            best_idx = i

    if best_idx is None or best_score < 4:
        return df

    new_header = [_normalize_column_name(v) for v in df.iloc[best_idx].tolist()]
    data = df.iloc[best_idx + 1 :].copy().reset_index(drop=True)

    n = min(len(new_header), data.shape[1])
    data = data.iloc[:, :n]
    new_header = new_header[:n]

    clean_cols = []
    seen = {}

    for c in new_header:
        c = _normalize_column_name(c)
        if not c or c.lower().startswith("unnamed:"):
            c = "col"

        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0

        clean_cols.append(c)

    data.columns = clean_cols
    return data


def _preview_df(df: pd.DataFrame, rows: int = 8) -> str:
    try:
        sample = df.head(rows).fillna("").astype(str).values.tolist()
        return str(sample)
    except Exception:
        return "<no preview>"


def _finalize_avantio_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError("Avantio: no se han podido leer datos (archivo vacío o formato no soportado).")

    df.columns = [_normalize_column_name(c) for c in df.columns]
    df = _rename_avantio_columns(df)

    detected = list(df.columns)
    missing = [c for c in REQUIRED_AVANTIO_COLS if c not in df.columns]

    if missing:
        raise ValueError(
            "Avantio: faltan columnas requeridas: "
            f"{missing}. Detectadas={detected[:50]}. "
            f"Preview primeras filas={_preview_df(df)}"
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


def _clean_avantio_html_tables(tables: list[pd.DataFrame]) -> pd.DataFrame:
    frames = []

    for t in tables:
        if t is None or t.empty:
            continue

        t = t.dropna(how="all").reset_index(drop=True)
        if t.empty:
            continue

        t = _promote_header_row(t)

        if t is None or t.empty:
            continue

        t.columns = [_normalize_column_name(c) for c in t.columns]
        t = _rename_avantio_columns(t)

        if "Alojamiento" in t.columns:
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


def _try_read_csv_with_sep(text: str, sep: str) -> pd.DataFrame | None:
    try:
        raw = pd.read_csv(
            StringIO(text),
            sep=sep,
            engine="python",
            header=None,
            on_bad_lines="skip"
        )
        if raw is None or raw.empty:
            return None

        raw = raw.dropna(how="all").reset_index(drop=True)
        df = _promote_header_row(raw)
        return df
    except Exception:
        return None


def _read_csv_robust(b: bytes) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

    for enc in encodings:
        try:
            text = b.decode(enc)
        except UnicodeDecodeError:
            continue

        seps = []
        try:
            sample = "\n".join(text.splitlines()[:20])
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            seps.append(dialect.delimiter)
        except Exception:
            pass

        for sep in [";", ",", "\t", "|"]:
            if sep not in seps:
                seps.append(sep)

        for sep in seps:
            df = _try_read_csv_with_sep(text, sep)
            if df is not None and not df.empty:
                return df

    raise ValueError("Avantio: no se pudo interpretar el CSV.")


def parse_avantio_entradas(uploaded_file) -> pd.DataFrame:
    b = uploaded_file.getvalue() if hasattr(uploaded_file, "getvalue") else uploaded_file.read()
    name = getattr(uploaded_file, "name", "") or ""
    lower_name = name.lower()

    # 1) CSV
    if lower_name.endswith(".csv"):
        df = _read_csv_robust(b)
        return _finalize_avantio_df(df)

    # 2) HTML disfrazado de XLS
    if _is_html_bytes(b):
        try:
            tables = pd.read_html(BytesIO(b), header=None)
            df = _clean_avantio_html_tables(tables)
            return _finalize_avantio_df(df)
        except Exception as e:
            raise ValueError(f"Avantio: error leyendo HTML/XLS: {e}") from e

    # 3) Excel real: intento con header=None primero
    try:
        raw = pd.read_excel(BytesIO(b), header=None)
        raw = raw.dropna(how="all").reset_index(drop=True)
        df = _promote_header_row(raw)
        df = _finalize_avantio_df(df)
        return df
    except Exception as e1:
        # fallback: lectura clásica por si el archivo ya trae cabecera real
        try:
            df = pd.read_excel(BytesIO(b))
            df = _finalize_avantio_df(df)
            return df
        except Exception as e2:
            raise ValueError(
                f"Avantio: error leyendo Excel. Intento1={e1} | Intento2={e2}"
            ) from e2


def parse_odoo_stock(uploaded_file) -> pd.DataFrame:
    b = uploaded_file.getvalue() if hasattr(uploaded_file, "getvalue") else uploaded_file.read()
    name = getattr(uploaded_file, "name", "") or ""

    if name.lower().endswith(".csv"):
        try:
            df = pd.read_csv(BytesIO(b))
        except Exception:
            text = b.decode("utf-8", errors="ignore")
            df = pd.read_csv(StringIO(text), sep=None, engine="python")
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
