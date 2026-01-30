import pandas as pd
import re
import unicodedata
from lxml import html


def _norm(s: str) -> str:
    s = str(s or "").strip()
    s = s.replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s


def _row_score_as_header(values: list[str]) -> int:
    txt = " | ".join(_norm(v) for v in values if v is not None)
    score = 0
    if "aloj" in txt:
        score += 3
    if "entrada" in txt and "fecha" in txt:
        score += 3
    if "salida" in txt and "fecha" in txt:
        score += 3
    if "check-in" in txt or "check in" in txt:
        score += 1
    if "check-out" in txt or "check out" in txt:
        score += 1
    if "cliente" in txt:
        score += 1
    return score


def _promote_best_header_row(df: pd.DataFrame, max_scan_rows: int = 15) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    cols_norm = [_norm(c) for c in df.columns]
    if any("aloj" in c for c in cols_norm) and any("entrada" in c for c in cols_norm):
        return df

    scan_n = min(len(df), max_scan_rows)
    best_i = None
    best_score = -1
    for i in range(scan_n):
        row_vals = df.iloc[i].astype(str).tolist()
        sc = _row_score_as_header(row_vals)
        if sc > best_score:
            best_score = sc
            best_i = i

    if best_i is None or best_score < 3:
        return df

    new_cols = df.iloc[best_i].astype(str).tolist()
    out = df.iloc[best_i + 1 :].copy()
    out.columns = [str(c).strip() for c in new_cols]
    out = out.reset_index(drop=True)
    return out


def _find_required_columns(df: pd.DataFrame):
    cols = list(df.columns)
    norm = {_norm(c): c for c in cols}

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
                for _ in range(colspan - 1):
                    row.append("")
            if row:
                max_len = max(max_len, len(row))
                rows.append(row)

        if not rows or max_len <= 3:
            continue

        norm_rows = [r + [""] * (max_len - len(r)) for r in rows]
        df = pd.DataFrame(norm_rows)
        df = _promote_best_header_row(df, max_scan_rows=15)
        dfs.append(df)

    return dfs


def parse_avantio_entradas(uploaded_file) -> pd.DataFrame:
    name = (uploaded_file.name or "").lower()

    if name.endswith(".xls") or name.endswith(".html"):
        raw = uploaded_file.getvalue()
        raw_html = None
        for enc in ("utf-8", "latin1", "cp1252"):
            raw_html = raw.decode(enc, errors="ignore")
            if raw_html:
                break

        dfs = _html_tables_to_dfs(raw_html)
        if not dfs:
            raise ValueError("Avantio: no se detectaron tablas en el archivo .xls/.html")

        best = None
        best_score = -1
        for d in dfs:
            d.columns = [str(c).strip() for c in d.columns]
            cols_norm = [_norm(c) for c in d.columns]
            score = sum([
                any("aloj" in c for c in cols_norm),
                any(("fecha" in c and "entrada" in c) for c in cols_norm),
                any(("fecha" in c and "salida" in c) for c in cols_norm),
            ])
            if score > best_score:
                best = d
                best_score = score

        df = best.copy()

    elif name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    df.columns = [str(c).strip() for c in df.columns]

    c_aloj, c_ent, c_sal = _find_required_columns(df)
    if not (c_aloj and c_ent and c_sal):
        raise ValueError(
            f"Avantio: faltan columnas requeridas. Detectadas={list(df.columns)[:50]} | "
            f"Encontradas: alojamiento={c_aloj}, entrada={c_ent}, salida={c_sal}"
        )

    df = df.rename(columns={
        c_aloj: "Alojamiento",
        c_ent: "Fecha entrada hora",
        c_sal: "Fecha salida hora",
    })

    df["Fecha_entrada_dt"] = pd.to_datetime(df["Fecha entrada hora"], errors="coerce", dayfirst=True)
    df["Fecha_salida_dt"] = pd.to_datetime(df["Fecha salida hora"], errors="coerce", dayfirst=True)

    return df


def parse_odoo_stock(uploaded_file) -> pd.DataFrame:
    """
    Devuelve SIEMPRE un DataFrame o lanza ValueError.
    Nunca devuelve None.
    """
    if uploaded_file is None:
        raise ValueError("Odoo: no se ha subido archivo.")

    name = (uploaded_file.name or "").lower()

    try:
        if name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
    except Exception as e:
        raise ValueError(f"Odoo: no pude leer el archivo ({e}).")

    if df is None or df.empty:
        raise ValueError("Odoo: el archivo está vacío o no tiene datos.")

    df.columns = [str(c).strip() for c in df.columns]

    # Heurística flexible de columnas
    col_ubi = col_prod = col_qty = None
    for c in df.columns:
        cl = _norm(c)
        if col_ubi is None and (("ubic" in cl) or ("location" in cl)):
            col_ubi = c
        if col_prod is None and (("producto" in cl) or ("product" in cl)):
            col_prod = c
        if col_qty is None and (("cantidad" in cl) or ("quantity" in cl) or ("qty" in cl) or ("on hand" in cl) or ("dispon" in cl)):
            col_qty = c

    if not (col_ubi and col_prod and col_qty):
        raise ValueError(f"Odoo: no detecto columnas. Encontradas: Ubicación={col_ubi}, Producto={col_prod}, Cantidad={col_qty}. Columnas={list(df.columns)}")

    df = df.rename(columns={
        col_ubi: "Ubicación",
        col_prod: "Producto",
        col_qty: "Cantidad",
    })

    # Limpieza
    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Ubicación"] = df["Ubicación"].astype(str).str.strip()
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0)

    # Quitar agrupaciones tipo "(123)" en ubicación
    df = df[~df["Ubicación"].str.contains(r"\(\d+\)", regex=True, na=False)].copy()

    # Quitar filas sin producto
    df = df[df["Producto"].notna() & (df["Producto"].str.strip() != "")].copy()

    if df.empty:
        raise ValueError("Odoo: tras limpiar, no quedó ninguna fila útil (Producto/Ubicación vacíos).")

    return df
