# src/gsheets_reports.py
from __future__ import annotations

import re
import math
from dataclasses import dataclass
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =========================
# Helpers de columnas tipo Excel
# =========================
def col_letter_to_idx0(letter: str) -> int:
    s = str(letter or "").strip().upper()
    if not re.fullmatch(r"[A-Z]+", s):
        raise ValueError(f"Letra de columna inválida: {letter!r}")
    n = 0
    for ch in s:
        n = n * 26 + (ord(ch) - 64)
    return n - 1  # A=0


def _safe_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and math.isnan(x):
        return ""
    return str(x)


def _normalize(s: Any) -> str:
    # similar a tu Apps Script: lower + quitar acentos + normalizar espacios
    import unicodedata

    t = _safe_str(s)
    t = unicodedata.normalize("NFD", t)
    t = "".join(ch for ch in t if unicodedata.category(ch) != "Mn")
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\b0+(\d)", r"\1", t)
    return t.strip().lower()


def _is_na_or_empty(v: Any) -> bool:
    t = _safe_str(v).strip()
    if not t:
        return True
    if re.fullmatch(r"n\/?a", t, flags=re.I):
        return True
    if t.lower() in {"na", "-", "—"}:
        return True
    if re.search(r"no\s*es\s*necesario", t, flags=re.I):
        return True
    return False


def _first_number_or_zero(v: Any) -> float:
    if v is None:
        return 0.0
    m = re.search(r"-?\d+(?:[.,]\d+)?", _safe_str(v).replace(",", "."))
    if not m:
        return 0.0
    try:
        return float(m.group(0))
    except Exception:
        return 0.0


def qty_or_flag(v: Any) -> int:
    """
    Para columnas:
    - si "No es necesario"/vacío -> 0
    - si número -> ese número
    - si texto tipo "Sí/OK/Finalizado/X" -> 1
    """
    if _is_na_or_empty(v):
        return 0
    n = _first_number_or_zero(v)
    if n > 0:
        return int(round(n))
    t = _safe_str(v).strip().lower()
    if re.fullmatch(r"(si|sí|ok|finalizado|true|x|1)", t, flags=re.I):
        return 1
    if re.search(r"(si|sí|finalizado|ok)", t, flags=re.I):
        return 1
    return 0


def _dt_floor_day(d: datetime) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0)


def _dt_ceil_day(d: datetime) -> datetime:
    return datetime(d.year, d.month, d.day, 23, 59, 59, 999999)


def _parse_datetime(v: Any) -> Optional[datetime]:
    # Google Forms suele venir como timestamp o string. Intentamos robusto.
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, 0, 0, 0)
    s = _safe_str(v).strip()
    if not s:
        return None

    # dd/mm/yyyy hh:mm:ss  o dd/mm/yyyy
    # También vale "7/05/2025 12:10:17"
    fmts = [
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            pass

    # último intento: pandas
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def cafe_texto(q: int, tipo: str) -> str:
    Q = int(q or 0)
    if Q <= 0:
        return "No es necesario"
    if not tipo:
        return str(Q)
    if tipo.strip().lower() == "molido":
        return f"{Q} Molido"
    return f"{Q} {'Cápsula' if Q==1 else 'Cápsulas'} {tipo}"


def join_non_empty(parts: List[str]) -> str:
    parts2 = [p for p in parts if p]
    return " · ".join(parts2)


# =========================
# Precios (bloque 4 y 5)
# =========================
DEFAULT_PRECIOS = {
    "Cápsula Nespresso": 0.18,
    "Cápsula Tassimo": 0.30,
    "Cápsula Dolce Gusto": 0.30,
    "Molido": 4.40,
    "Azúcar": 1.55,
    "Sal": 0.75,
    "Té/Infusiones": 1.25,
    "Detergente": 2.50,
    "Insecticida": 2.80,
    "Gel ducha": 4.80,
    "Shampoo": 4.80,
    "Jabón manos": 1.10,
    "Escoba": 1.80,
    "Mocho": 1.50,
    "Sal lavavajillas": 0.95,
    "Vinagre": 0.90,
    "Abrillantador": 1.25,
    "Kit cocina": 1.00,
    "Papel higiénico": 0.37,
    "Botella agua": 0.30,
}


def _producto_key_from_desc(desc: str) -> Optional[str]:
    d = desc.lower()

    if "nespresso" in d:
        return "Cápsula Nespresso"
    if "tassimo" in d:
        return "Cápsula Tassimo"
    if "dolce gusto" in d:
        return "Cápsula Dolce Gusto"
    if "molido" in d:
        return "Molido"

    # equivalencias por texto
    # (en tu HTML usabas "Té" pero en reposición lo llamas "Té/Infusiones")
    if "azúcar" in d or "azucar" in d:
        return "Azúcar"
    if re.search(r"\bsal\b", d):
        return "Sal"
    if "té" in d or "te/infus" in d or "infusion" in d or "infusión" in d:
        return "Té/Infusiones"
    if "insecticida" in d:
        return "Insecticida"
    if "gel ducha" in d or "gel de ducha" in d:
        return "Gel ducha"
    if "shampoo" in d or "champú" in d or "champu" in d:
        return "Shampoo"
    if "jabón manos" in d or "jabon manos" in d or "gel manos" in d or "jabón de manos" in d:
        return "Jabón manos"
    if "escoba" in d:
        return "Escoba"
    if "mocho" in d:
        return "Mocho"
    if "detergente" in d:
        return "Detergente"
    if "sal lavavajillas" in d:
        return "Sal lavavajillas"
    if "vinagre" in d:
        return "Vinagre"
    if "abrillantador" in d:
        return "Abrillantador"
    if "kit cocina" in d:
        return "Kit cocina"
    if "papel hig" in d:
        return "Papel higiénico"
    if "botella agua" in d or "agua" == d.strip():
        return "Botella agua"

    return None


# =========================
# Report principal
# =========================
@dataclass
class CleaningReport:
    incidencias: pd.DataFrame
    faltantes: pd.DataFrame
    detalle: pd.DataFrame
    incidencias_por_apt: pd.DataFrame
    costes_producto: pd.DataFrame
    costes_por_apt: pd.DataFrame


def build_cleaning_report(
    sheet_df: pd.DataFrame,
    coffee_type_by_apt: Dict[str, str],
    *,
    from_dt: Optional[datetime] = None,
    to_dt: Optional[datetime] = None,
    alojamiento_contains: str = "",
    responsable_contains: str = "",
    precios: Optional[Dict[str, float]] = None,
) -> CleaningReport:
    """
    Replica la lógica del Apps Script:
    - Marca temporal: filtra por fecha
    - Apartamento / Si es otro piso indicar aqui
    - Responsable
    - Incidencias / tareas a realizar
    - Faltantes por entrada: col P
    - Reposiciones: Q..AH (incluye flags)
    """

    precios = dict(precios or DEFAULT_PRECIOS)

    df = sheet_df.copy()
    # Normalizar nombres de columnas
    df.columns = [str(c).strip() for c in df.columns]

    # Buscar columnas por nombre (como en tu script)
    def find_col(regex: str) -> Optional[str]:
        for c in df.columns:
            if re.search(regex, str(c), flags=re.I):
                return c
        return None

    c_fecha = find_col(r"^marca\s+temporal$")
    c_apt = find_col(r"^apartamento$")
    c_alt = find_col(r"^si\s+es\s+otro\s+piso\s+indicar\s+aqui$")
    c_resp = find_col(r"^responsable$")
    c_incid = find_col(r"^incidencias\/tareas\s+a\s+realizar$")

    if not c_fecha or not c_apt:
        raise ValueError(
            f"En Google Sheet faltan columnas clave. Detectadas: fecha={c_fecha}, apt={c_apt}. Columnas: {list(df.columns)}"
        )

    # Parse datetime
    df["_dt"] = df[c_fecha].apply(_parse_datetime)
    df = df[df["_dt"].notna()].copy()
    df["_dt"] = pd.to_datetime(df["_dt"])

    if from_dt:
        df = df[df["_dt"] >= _dt_floor_day(from_dt)].copy()
    if to_dt:
        df = df[df["_dt"] <= _dt_ceil_day(to_dt)].copy()

    # Alojamiento final
    def _aloj(row) -> str:
        apt_base = _safe_str(row.get(c_apt, "")).strip()
        alt = _safe_str(row.get(c_alt, "")).strip() if c_alt else ""
        if _normalize(apt_base) == "otro" and alt:
            return alt
        return apt_base

    df["Alojamiento"] = df.apply(_aloj, axis=1)
    df["Responsable"] = df[c_resp].apply(lambda x: _safe_str(x).strip()) if c_resp else ""
    df["Incidencias"] = df[c_incid].apply(lambda x: _safe_str(x).strip()) if c_incid else ""

    # Filtros texto
    if alojamiento_contains:
        needle = alojamiento_contains.strip().lower()
        df = df[df["Alojamiento"].str.lower().str.contains(needle, na=False)].copy()
    if responsable_contains:
        needle = responsable_contains.strip().lower()
        df = df[df["Responsable"].str.lower().str.contains(needle, na=False)].copy()

    # Índices por letra (P..AH) usando posición en dataframe (0-based)
    iP = col_letter_to_idx0("P")
    iQ = col_letter_to_idx0("Q")
    iR = col_letter_to_idx0("R")
    iS = col_letter_to_idx0("S")
    iT = col_letter_to_idx0("T")
    iU = col_letter_to_idx0("U")
    iV = col_letter_to_idx0("V")
    iW = col_letter_to_idx0("W")
    iX = col_letter_to_idx0("X")
    iY = col_letter_to_idx0("Y")
    iZ = col_letter_to_idx0("Z")
    iAA = col_letter_to_idx0("AA")
    iAB = col_letter_to_idx0("AB")
    iAC = col_letter_to_idx0("AC")
    iAD = col_letter_to_idx0("AD")
    iAE = col_letter_to_idx0("AE")
    iAF = col_letter_to_idx0("AF")
    iAG = col_letter_to_idx0("AG")
    iAH = col_letter_to_idx0("AH")

    # Para acceder por índice, usamos .iloc
    def cell(row_idx: int, col_idx: int) -> Any:
        if col_idx < 0 or col_idx >= df.shape[1]:
            return ""
        return df.iloc[row_idx, col_idx]

    incidencias_rows = []
    faltantes_rows = []
    detalle_rows = []

    for ridx in range(len(df)):
        row = df.iloc[ridx]
        dt = row["_dt"].to_pydatetime()
        aloj = row["Alojamiento"]
        resp = row["Responsable"]
        inc = row["Incidencias"]

        # 1) incidencias
        if inc:
            incidencias_rows.append(
                {"Fecha": dt.date().isoformat(), "Alojamiento": aloj, "Responsable": resp, "Incidencias / Tareas": inc}
            )

        # 2) faltantes (P)
        falt_raw = cell(ridx, iP)
        falt = _safe_str(falt_raw).strip()
        if not _is_na_or_empty(falt):
            faltantes_rows.append({"Fecha": dt.date().isoformat(), "Alojamiento": aloj, "Faltantes": falt})

        # 3) detalle
        tipo = coffee_type_by_apt.get(_normalize(aloj), "")
        qty_caps = qty_or_flag(cell(ridx, iQ))
        cafe_txt = cafe_texto(qty_caps, tipo)

        parts: List[str] = []
        # Café
        if cafe_txt != "No es necesario":
            parts.append(cafe_txt)

        # Q..AC
        qAz = qty_or_flag(cell(ridx, iR))
        if qAz > 0:
            parts.append(f"{qAz} Azúcar")

        qSal = qty_or_flag(cell(ridx, iS))
        if qSal > 0:
            parts.append(f"{qSal} Sal")

        qTe = qty_or_flag(cell(ridx, iT))
        if qTe > 0:
            parts.append(f"{qTe} Té/Infusiones")

        qIns = qty_or_flag(cell(ridx, iU))
        if qIns > 0:
            parts.append(f"{qIns} Insecticida")

        qGel = qty_or_flag(cell(ridx, iV))
        if qGel > 0:
            parts.append(f"{qGel} Gel ducha")

        qSha = qty_or_flag(cell(ridx, iW))
        if qSha > 0:
            parts.append(f"{qSha} Shampoo")

        qEsc = qty_or_flag(cell(ridx, iX))
        if qEsc > 0:
            parts.append(f"{qEsc} Escoba")

        qMoc = qty_or_flag(cell(ridx, iY))
        if qMoc > 0:
            parts.append(f"{qMoc} Mocho")

        qDes = qty_or_flag(cell(ridx, iZ))
        if qDes > 0:
            parts.append(f"{qDes} Pastilla descalcificadora")

        qKit = qty_or_flag(cell(ridx, iAA))
        if qKit > 0:
            parts.append(f"{qKit} Kit cocina")

        qPap = qty_or_flag(cell(ridx, iAB))
        if qPap > 0:
            parts.append(f"{qPap} Papel higiénico")

        qAgua = qty_or_flag(cell(ridx, iAC))
        if qAgua > 0:
            parts.append(f"{qAgua} Botella agua")

        # AD..AH flags
        qDet = qty_or_flag(cell(ridx, iAD))
        if qDet > 0:
            parts.append(f"{qDet} Detergente")

        qJab = qty_or_flag(cell(ridx, iAE))
        if qJab > 0:
            parts.append(f"{qJab} Jabón manos")

        qSLV = qty_or_flag(cell(ridx, iAF))
        if qSLV > 0:
            parts.append(f"{qSLV} Sal lavavajillas")

        qVin = qty_or_flag(cell(ridx, iAG))
        if qVin > 0:
            parts.append(f"{qVin} Vinagre")

        qAbr = qty_or_flag(cell(ridx, iAH))
        if qAbr > 0:
            parts.append(f"{qAbr} Abrillantador")

        repos = join_non_empty(parts) or "No es necesario"

        detalle_rows.append(
            {
                "Fecha": dt.date().isoformat(),
                "Alojamiento": aloj,
                "Responsable": resp,
                "Café": cafe_txt,
                "Tipo café": tipo,
                "Reposiciones": repos,
                "Faltantes entrada": (falt if not _is_na_or_empty(falt) else ""),
            }
        )

    incidencias_df = pd.DataFrame(incidencias_rows)
    faltantes_df = pd.DataFrame(faltantes_rows)
    detalle_df = pd.DataFrame(detalle_rows)

    # 6) incidencias por apto
    if not incidencias_df.empty:
        inc_por_apt = (
            incidencias_df.groupby("Alojamiento", as_index=False)
            .size()
            .rename(columns={"size": "Incidencias"})
            .sort_values("Incidencias", ascending=False)
        )
    else:
        inc_por_apt = pd.DataFrame(columns=["Alojamiento", "Incidencias"])

    # 4 y 5) costes: parsear "Reposiciones" estilo "3 Azúcar · 56 Cápsulas Tassimo"
    def parse_parts(repos_str: str) -> List[Tuple[int, str]]:
        if not repos_str or re.search(r"no\s+es\s+necesario", repos_str, flags=re.I):
            return []
        parts = [p.strip() for p in repos_str.split("·")]
        out = []
        for p in parts:
            m = re.match(r"^(\d+)\s+(.*)$", p.strip())
            if not m:
                continue
            qty = int(m.group(1))
            desc = m.group(2).strip()
            out.append((qty, desc))
        return out

    # agregado por producto
    acumulado: Dict[str, int] = {}
    coste_por_apt: Dict[str, float] = {}

    for _, r in detalle_df.iterrows():
        aloj = _safe_str(r.get("Alojamiento", "")).strip()
        repos = _safe_str(r.get("Reposiciones", "")).strip()
        coste_fila = 0.0

        for qty, desc in parse_parts(repos):
            key = _producto_key_from_desc(desc)
            if not key:
                continue

            # regla HTML: azúcar y té -> si aparece, cuenta 1 (no múltiplos)
            if key in {"Azúcar", "Té/Infusiones"}:
                qty = 1 if qty > 0 else 0

            acumulado[key] = acumulado.get(key, 0) + qty
            coste_fila += qty * float(precios.get(key, 0.0))

        if aloj:
            coste_por_apt[aloj] = coste_por_apt.get(aloj, 0.0) + coste_fila

    costes_producto = (
        pd.DataFrame(
            [{"Producto": k, "Unidades": v, "Precio unitario": precios.get(k, 0.0), "Coste": v * precios.get(k, 0.0)}
             for k, v in acumulado.items()]
        )
        if acumulado
        else pd.DataFrame(columns=["Producto", "Unidades", "Precio unitario", "Coste"])
    )

    if not costes_producto.empty:
        costes_producto = costes_producto.sort_values("Coste", ascending=False)

    costes_por_apt_df = (
        pd.DataFrame([{"Alojamiento": k, "Coste total (€)": v} for k, v in coste_por_apt.items()])
        if coste_por_apt
        else pd.DataFrame(columns=["Alojamiento", "Coste total (€)"])
    )
    if not costes_por_apt_df.empty:
        costes_por_apt_df = costes_por_apt_df.sort_values("Coste total (€)", ascending=False)

    return CleaningReport(
        incidencias=incidencias_df,
        faltantes=faltantes_df,
        detalle=detalle_df,
        incidencias_por_apt=inc_por_apt,
        costes_producto=costes_producto,
        costes_por_apt=costes_por_apt_df,
    )
