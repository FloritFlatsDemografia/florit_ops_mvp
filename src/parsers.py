import pandas as pd
import re
from io import BytesIO
from datetime import datetime

def parse_avantio_entradas(uploaded_file) -> pd.DataFrame:
    name = (uploaded_file.name or "").lower()

    # Avantio "xls" export is often HTML table.
    if name.endswith(".xls") or name.endswith(".html"):
        content = uploaded_file.getvalue()
        # read_html expects bytes decoded to str
        html = content.decode("utf-8", errors="ignore")
        tables = pd.read_html(html)
        # Heuristic: pick the table that contains 'Alojamiento' and 'Fecha entrada hora'
        best = None
        for t in tables:
            cols = [str(c).strip() for c in t.columns]
            if ("Alojamiento" in cols) and ("Fecha entrada hora" in cols) and ("Fecha salida hora" in cols):
                best = t
                break
        if best is None:
            best = tables[-1]
        df = best.copy()
    elif name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    # Standardize col names (keep original Spanish but ensure exact)
    df.columns = [str(c).strip() for c in df.columns]

    needed = ["Alojamiento","Fecha entrada hora","Fecha salida hora"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Avantio: faltan columnas requeridas: {missing}")

    # Parse datetimes
    df["Fecha_entrada_dt"] = pd.to_datetime(df["Fecha entrada hora"], errors="coerce", dayfirst=True)
    df["Fecha_salida_dt"] = pd.to_datetime(df["Fecha salida hora"], errors="coerce", dayfirst=True)

    return df

def parse_odoo_stock(uploaded_file) -> pd.DataFrame:
    name = (uploaded_file.name or "").lower()
    if name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    df.columns = [str(c).strip() for c in df.columns]

    # Common columns in Odoo exports: Ubicación, Producto, Cantidad
    # Your sample matches these names.
    colmap = {}
    for c in df.columns:
        cl = c.lower()
        if cl in ["ubicación","ubicacion","location","ubicacion completa","ubicación completa"]:
            colmap[c] = "Ubicación"
        elif cl in ["producto","product","product name","product/variant"]:
            colmap[c] = "Producto"
        elif cl in ["cantidad","quantity","on hand","qty","disponible"]:
            colmap[c] = "Cantidad"
    df = df.rename(columns=colmap)

    required = ["Ubicación","Producto","Cantidad"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError(f"Odoo: faltan columnas requeridas: {miss}")

    # Filter out grouping/header rows: those have Producto empty OR Ubicación contains "(n)"
    df["Producto"] = df["Producto"].astype("string")
    df = df[df["Producto"].notna() & (df["Producto"].str.strip() != "")].copy()
    df = df[~df["Ubicación"].astype(str).str.contains(r"\(\d+\)", regex=True, na=False)].copy()

    # Clean
    df["Ubicación"] = df["Ubicación"].astype(str).str.strip()
    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0)

    return df
