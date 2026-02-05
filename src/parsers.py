import pandas as pd
from io import BytesIO
import re


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

        # Normalizamos: quitamos filas totalmente vacías
        t = t.dropna(how="all")
        if t.empty:
            continue

        # Buscamos fila cabecera real: donde aparezca "ID Reserva"
        # (suele estar en la primera columna)
        header_idx = None
        for i in range(min(len(t), 15)):
            row = t.iloc[i].astype(str).str.strip().tolist()
            if any(x == "ID Reserva" for x in row):
                header_idx = i
                break

        if header_idx is None:
            continue

        header = t.iloc[header_idx].astype(str).str.strip().tolist()
        data = t.iloc[header_idx + 1 :].copy()

        # Si la tabla está “desplazada”, forzamos igual nº columnas
        if data.shape[1] != len(header):
            # Ajuste defensivo: recorta o rellena
            n = min(data.shape[1], len(header))
            data = data.iloc[:, :n]
            header = header[:n]

        data.columns = header

        # Filtramos filas basura (cabeceras repetidas, etc.)
        if "ID Reserva" in data.columns:
            data = data[data["ID Reserva"].astype(str).str.strip().ne("ID Reserva")]

        # Quitamos filas sin alojamiento (a veces hay separadores)
        if "Alojamiento" in data.columns:
            data = data[data["Alojamiento"].notna()]

        if not data.empty:
            frames.append(data)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Limpieza básica de texto
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
            df[c] = df[c].replace({"nan": None, "None": None, "": None})

    return df


def parse_avantio_entradas(uploaded_file) -> pd.DataFrame:
    """
    Acepta:
      - .xls HTML (Avantio típico)
      - .xlsx real
      - .csv
    Devuelve DataFrame con las columnas originales de Avantio (incluyendo REQUIRED_AVANTIO_COLS).
    """
    # Leemos bytes
    b = uploaded_file.getvalue() if hasattr(uploaded_file, "getvalue") else uploaded_file.read()

    # Caso CSV
    name = getattr(uploaded_file, "name", "") or ""
    if name.lower().endswith(".csv"):
        df = pd.read_csv(BytesIO(b))
    else:
        # Caso HTML disguised .xls
        if _is_html_bytes(b):
            tables = pd.read_html(BytesIO(b))
            df = _clean_avantio_html_tables(tables)
        else:
            # Excel real
            df = pd.read_excel(BytesIO(b))

    if df is None or df.empty:
        raise ValueError("Avantio: no se han podido leer datos (archivo vacío o formato no soportado).")

    # Normalizamos nombres por si vienen con espacios raros
    df.columns = [str(c).strip() for c in df.columns]

    # Validación columnas requeridas
    detected = list(df.columns)
    missing = [c for c in REQUIRED_AVANTIO_COLS if c not in df.columns]

    if missing:
        # Debug claro
        raise ValueError(
            f"Avantio: faltan columnas requeridas: {missing}. Detectadas={detected[:50]}"
        )

    # Convertimos fechas (dejamos como columna original, pero que pandas pueda trabajar)
    df["Fecha entrada hora"] = pd.to_datetime(df["Fecha entrada hora"], errors="coerce", dayfirst=True)
    df["Fecha salida hora"] = pd.to_datetime(df["Fecha salida hora"], errors="coerce", dayfirst=True)

    # Limpieza alojamiento
    df["Alojamiento"] = df["Alojamiento"].astype(str).str.strip()

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
        df = pd.read_csv(BytesIO(b))
    else:
        df = pd.read_excel(BytesIO(b))

    if df is None or df.empty:
        return pd.DataFrame()

    df.columns = [str(c).strip() for c in df.columns]

    # Intento de detección flexible
    col_ubic = None
    for c in df.columns:
        if c.lower() in ["ubicación", "ubicacion", "location", "ubicacion/stock", "ubicación"]:
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

    # Cantidad numérica
    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0)

    # Limpieza
    out["Ubicación"] = out["Ubicación"].astype(str).str.strip()
    out["Producto"] = out["Producto"].astype(str).str.strip()

    return out
