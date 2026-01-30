import pandas as pd
import re
import unicodedata

from lxml import html


def _norm_col(s: str) -> str:
    s = str(s or "").strip()
    s = s.replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    s = "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
    return s


def _coerce_header_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Si las cabeceras vienen dentro de la primera fila, las promovemos.
    """
    if df is None or df.empty:
        return df

    # Si las columnas son numéricas/unnamed, huele a header mal interpretado
    col_norm = [_norm_col(c) for c in df.columns]
    looks_bad = all(c.startswith("unnamed") or c.isdigit() or c == "" for c in col_norm)

    if looks_bad and len(df) >= 2:
        first_row = df.iloc[0].astype(str).tolist()
        # señales típicas de cabecera
        if any("aloj" in _norm_col(x) or ("fecha" in _norm_col(x) and ("entrada" in _norm_col(x) or "salida" in _norm_col(x))) for x in first_row):
            df2 = df.copy()
            df2.columns = first_row
            df2 = df2.iloc[1:].copy()
            df2.columns = [str(c).strip() for c in df2.columns]
            return df2

    return df


def _find_required_columns(df: pd.DataFrame):
    """
    Encuentra columnas equivalentes aunque varíen.
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


def _html_tables_to_dfs(raw_html: str) -> list[pd.DataFrame]:
    """
    Parser HTML robusto con lxml:
    - extrae textos de th/td
    - respeta colspan (repitiendo celdas en columnas adicionales)
    """
    root = html.fromstring(raw_html)
    tables = root.xpath("//table")
    dfs = []

    for t in tables:
        rows = []
        max_len = 0

        for tr in t.xpath(".//tr"):
            cells = tr.xpath("./th|./td")
            row = []
            for cell in cells:
                txt = cell.text_content().strip()
                colspan = cell.get("colspan")
                try:
                    colspan = int(colspan) if colspan else 1
                except Exception:
                    colspan = 1

                row.append(txt)
                # si hay colspan, añadimos placeholders extra
                for _ in range(colspan - 1):
                    row.append("")

            if row:
                max_len = max(max_len, len(row))
                rows.append(row)

        if not rows or max_len <= 1:
            continue

        # pad para misma longitud
        norm_rows = [r + [""] * (max_len - len(r)) for r in rows]
        df = pd.DataFrame(norm_rows)
        df = _coerce_header_row(df)
        dfs.append(df)

    return dfs


def parse_avantio_entradas(uploaded_file) -> pd.DataFrame:
    name = (uploaded_file.name or "").lower()

    if name.endswith(".xls") or name.endswith(".html"):
        raw = uploaded_file.getvalue()

        # decode flexible
        raw_html = None
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                raw_html = raw.decode(enc, errors="ignore")
                break
            except Exception:
                continue

        # 1) lxml manual parse
        dfs = _html_tables_to_dfs(raw_html)

        # 2) si lxml no encontró nada útil, fallback a read_html
        if not dfs:
            try:
                tables = pd.read_html(raw_html)
                dfs = [ _coerce_header_row(t) for t in tables ]
            except Exception:
                dfs = []

        if not dfs:
            raise ValueError("Avantio: no se detectaron tablas en el archivo .xls/.html")

        # seleccionar la tabla más probable por score
        best = None
        best_score = -1
        for df in dfs:
            df.columns = [str(c).strip() for c in df.columns]
            cols_norm = [_norm_col(c) for c in df.columns]
            score = sum([
                any("aloj" in c for c in cols_norm),
                any(("fecha" in c and "entrada" in c) for c in cols_norm),
                any(("fecha" in c and "salida" in c) for c in cols_norm),
            ])
            if score > best_score:
                best = df
                best_score = score

        df = best.copy()

    elif name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        # .xlsx o similar
        df = pd.read_excel(uploaded_file)

    df = _coerce_header_row(df)
    df.columns = [str(c).strip() for c in df.columns]

    c_aloj, c_ent, c_sal = _find_required_columns(df)
    if not (c_aloj and c_ent and c_sal):
        raise ValueError(
            f"Avantio: faltan columnas requeridas. Detectadas={list(df.columns)[:50]} | "
            f"Encontradas: alojamiento={c_aloj}, entrada={c_ent}, salida={c_sal}"
        )

    # Renombrar a estándar interno
    df = df.rename(columns={
        c_aloj: "Alojamiento",
        c_ent: "Fecha entrada hora",
        c_sal: "Fecha salida hora",
    })

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

    # Filtrar filas "cabecera" y ubicaciones tipo "(21)"
    df["Producto"] = df["Producto"].astype("string")
    df = df[df["Producto"].notna() & (df["Producto"].str.strip() != "")].copy()
    df = df[~df["Ubicación"].astype(str).str.contains(r"\(\d+\)", regex=True, na=False)].copy()

    df["Ubicación"] = df["Ubicación"].astype(str).str.strip()
    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0)

    return df
