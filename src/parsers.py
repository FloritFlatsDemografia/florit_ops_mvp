import pandas as pd
import re
import unicodedata


def _norm_col(s: str) -> str:
    s = str(s or "").strip()
    s = s.replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s


def _coerce_header_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Si el HTML/XLS viene con cabeceras dentro de la primera fila, las promovemos a header.
    """
    if df is None or df.empty:
        return df

    col_norm = [_norm_col(c) for c in df.columns]
    looks_bad = all(c.startswith("unnamed") or c.isdigit() or c == "" for c in col_norm)

    if looks_bad and len(df) >= 2:
        first_row = df.iloc[0].astype(str).tolist()
        # Si en la primera fila hay palabras típicas de cabecera
        if any(("aloj" in _norm_col(x) or ("fecha" in _norm_col(x) and ("entrada" in _norm_col(x) or "salida" in _norm_col(x)))) for x in first_row):
            df2 = df.copy()
            df2.columns = first_row
            df2 = df2.iloc[1:].copy()
            df2.columns = [str(c).strip() for c in df2.columns]
            return df2

    return df


def _find_required_columns(df: pd.DataFrame):
    """
    Encuentra columnas equivalentes aunque cambien levemente.
    Requisitos:
    - Alojamiento (contiene 'aloj')
    - Fecha entrada (contiene 'fecha' y 'entrada')
    - Fecha salida (contiene 'fecha' y 'salida')
    """
    cols = list(df.columns)
    norm = {_norm_col(c): c for c in cols}

    def pick(patterns):
        for nc, orig in norm.items():
            ok = True
            for p in patterns:
                if re.search(p, nc) is None:
                    ok = False
                    break
            if ok:
                return orig
        return None

    c_aloj = pick([r"aloj"])
    c_ent = pick([r"fecha", r"entrada"])
    c_sal = pick([r"fecha", r"salida"])
    return c_aloj, c_ent, c_sal


def _explode_single_column_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Si df tiene 1 columna (p.ej. '0') con todo el contenido concatenado,
    intentamos separarlo por el delimitador correcto y reconstruir la tabla.
    """
    if df is None or df.empty or df.shape[1] != 1:
        return df

    col = df.columns[0]
    s = df[col].astype(str)

    # Elegir separador con mejor "split"
    seps = ["\t", ";", "|", ","]
    best_sep = None
    best_parts = 0

    sample = s.iloc[0] if len(s) else ""
    for sep in seps:
        parts = len(sample.split(sep))
        if parts > best_parts:
            best_parts = parts
            best_sep = sep

    # Si no hay splits útiles, devolvemos sin tocar
    if best_sep is None or best_parts < 3:
        return df

    expanded = s.str.split(best_sep, expand=True)

    # Promover primera fila como header si tiene pinta de cabecera
    expanded = _coerce_header_row(expanded)
    return expanded


def parse_avantio_entradas(uploaded_file) -> pd.DataFrame:
    name = (uploaded_file.name or "").lower()

    # 1) Leer como HTML si es .xls/.html (Avantio suele exportar así)
    if name.endswith(".xls") or name.endswith(".html"):
        raw = uploaded_file.getvalue()

        # Probar decodificaciones típicas
        html = None
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                html = raw.decode(enc, errors="ignore")
                break
            except Exception:
                continue

        tables = pd.read_html(html)

        # Elegir la tabla más probable
        best = None
        best_score = -1
        for t in tables:
            t = _coerce_header_row(t)

            # Si viene en 1 columna, intentamos explotar
            if t.shape[1] == 1:
                t = _explode_single_column_table(t)

            cols_norm = [_norm_col(c) for c in t.columns]
            score = sum([
                any("aloj" in c for c in cols_norm),
                any(("fecha" in c and "entrada" in c) for c in cols_norm),
                any(("fecha" in c and "salida" in c) for c in cols_norm),
            ])

            if score > best_score:
                best = t
                best_score = score

        df = best.copy()

    elif name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    # 2) Normalizar cabeceras / explotar si siguiera en 1 columna
    df = _coerce_header_row(df)
    if df.shape[1] == 1:
        df = _explode_single_column_table(df)

    df.columns = [str(c).strip() for c in df.columns]

    # 3) Detectar columnas clave
    c_aloj, c_ent, c_sal = _find_required_columns(df)
    if not (c_aloj and c_ent and c_sal):
        raise ValueError(
            f"Avantio: faltan columnas requeridas. Detectadas={list(df.columns)[:50]} | "
            f"Encontradas: alojamiento={c_aloj}, entrada={c_ent}, salida={c_sal}"
        )

    # 4) Renombrar a estándar interno
    df = df.rename(columns={
        c_aloj: "Alojamiento",
        c_ent: "Fecha entrada hora",
        c_sal: "Fecha salida hora",
    })

    # 5) Parse datetimes
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

    # Columnas típicas Odoo: Ubicación, Producto, Cantidad
    colmap = {}
    for c in df.columns:
        cl = _norm_col(c)
        if cl in ["ubicacion", "ubicacion completa", "location"]:
            colmap[c] = "Ubicación"
        elif cl in ["producto", "product", "product name", "product/variant"]:
            colmap[c] = "Producto"
        elif cl in ["cantidad", "quantity", "on hand", "qty", "disponible"]:
            colmap[c] = "Cantidad"
    df = df.rename(columns=colmap)

    required = ["Ubicación", "Producto", "Cantidad"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError(f"Odoo: faltan columnas requeridas: {miss}. Columnas={list(df.columns)}")

    # Filtrar filas "cabecera" (Producto vacío) y ubicaciones tipo "(21)"
    df["Producto"] = df["Producto"].astype("string")
    df = df[df["Producto"].notna() & (df["Producto"].str.strip() != "")].copy()
    df = df[~df["Ubicación"].astype(str).str.contains(r"\(\d+\)", regex=True, na=False)].copy()

    df["Ubicación"] = df["Ubicación"].astype(str).str.strip()
    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0)

    return df
