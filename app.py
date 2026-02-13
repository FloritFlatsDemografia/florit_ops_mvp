import streamlit as st
import pandas as pd
from zoneinfo import ZoneInfo
from urllib.parse import quote
import re
import unicodedata
import os

ORIGIN_LAT = 39.45702028460933
ORIGIN_LNG = -0.38498336081567713

# ✅ Ajuste solicitado: ventana para considerar "apto/listo" según última limpieza
CLEAN_READY_LOOKBACK_DAYS = 3  # <- antes 5, ahora 3


# =========================
# Apartamento key (matching robusto)
# =========================
def _apt_key(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # quita tildes
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b0+(\d)", r"\1", s)  # "APOLO 029" -> "APOLO 29"
    return s.upper().strip()


# =========================
# Excel-letter column helper
# =========================
def _col_by_excel_letter(df: pd.DataFrame, letter: str) -> str:
    letter = letter.upper().strip()
    idx = 0
    for ch in letter:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    idx -= 1
    if idx < 0 or idx >= len(df.columns):
        raise KeyError(f"No existe la columna {letter} en el DF (tamaño {len(df.columns)}).")
    return df.columns[idx]


# =========================
# Hora check-in (default 16:00)
# =========================
def _parse_time_to_hhmm(x) -> str:
    if x is None:
        return "16:00"
    try:
        if isinstance(x, float) and pd.isna(x):
            return "16:00"
    except Exception:
        pass

    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return "16:00"

    s = s.lower().replace("h", "").replace(".", ":").strip()

    s_num = s.replace(",", ".")
    try:
        if ":" not in s_num and re.fullmatch(r"[0-9]+(\.[0-9]+)?", s_num):
            hh = int(float(s_num))
            hh = max(0, min(23, hh))
            return f"{hh:02d}:00"
    except Exception:
        pass

    try:
        parts = s.split(":")
        if len(parts) >= 2:
            hh = int(parts[0])
            mm = int(parts[1])
            hh = max(0, min(23, hh))
            mm = max(0, min(59, mm))
            return f"{hh:02d}:{mm:02d}"
        if len(parts) == 1 and parts[0].isdigit():
            hh = int(parts[0])
            hh = max(0, min(23, hh))
            return f"{hh:02d}:00"
    except Exception:
        pass

    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%H:%M")
    except Exception:
        pass

    return "16:00"


# =========================
# Teléfono helpers
# =========================
def _clean_phone(x) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, float) and pd.isna(x):
            return ""
    except Exception:
        pass

    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""

    s = s.replace("\u00A0", " ").strip()
    has_plus = s.startswith("+")
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return ""
    return ("+" if has_plus else "") + digits


def _wa_phone_digits(phone: str) -> str:
    """
    WhatsApp 'phone' param: dígitos con prefijo país, SIN '+'.
    """
    if phone is None:
        return ""
    s = str(phone).strip()
    if not s:
        return ""
    s = _clean_phone(s)
    s = s.replace("+", "")
    s = re.sub(r"\D+", "", s)
    return s


def _first_name(cliente: str) -> str:
    if cliente is None:
        return ""
    s = str(cliente).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    return s.split()[0].strip().title()


# =========================
# WhatsApp helpers
# =========================
def _wa_send_url(phone_digits: str, text: str) -> str | None:
    """
    Abre WhatsApp (web/app) con teléfono + texto.
    """
    phone_digits = (phone_digits or "").strip()
    if not phone_digits:
        return None
    msg = (text or "").strip()
    if not msg:
        msg = ""
    base = "https://api.whatsapp.com/send"
    return f"{base}?phone={quote(phone_digits)}&text={quote(msg)}&type=phone_number&app_absent=0"


def _safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, float) and pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    if s.lower() in {"nan", "none"}:
        return ""
    return s


def _compose_wa_message(nombre: str, body: str, url_maps: str, url_youtube: str, lang: str) -> str:
    """
    Mensaje final (instrucciones):
    - Hola/Hi {Nombre}!
    - body (WA_ES/WA_EN)
    - Maps
    - YouTube
    """
    body = _safe_str(body)
    url_maps = _safe_str(url_maps)
    url_youtube = _safe_str(url_youtube)

    greet = ""
    if nombre:
        if lang.upper() == "EN":
            greet = f"Hi {nombre}!\n\n"
        else:
            greet = f"Hola {nombre}!\n\n"

    parts = []
    if greet:
        parts.append(greet.rstrip())

    if body:
        parts.append(body.strip())

    if url_maps:
        parts.append(f"📍 Google Maps: {url_maps}")

    if url_youtube:
        if lang.upper() == "EN":
            parts.append(f"🎥 Video: {url_youtube}")
        else:
            parts.append(f"🎥 Vídeo: {url_youtube}")

    return "\n\n".join([p for p in parts if p and p.strip()])


def _compose_first_contact(nombre: str, body: str, lang: str) -> str:
    """
    Mensaje final (primer contacto):
    - Hola/Hi {Nombre}!
    - body (PRIMER_CONTACTO_ES/EN)
    """
    body = _safe_str(body)

    greet = ""
    if nombre:
        if lang.upper() == "EN":
            greet = f"Hi {nombre}!\n\n"
        else:
            greet = f"Hola {nombre}!\n\n"

    parts = []
    if greet:
        parts.append(greet.rstrip())
    if body:
        parts.append(body.strip())

    return "\n\n".join([p for p in parts if p and p.strip()])


def load_whatsapp_master_from_data() -> pd.DataFrame:
    """
    Espera archivo: data/whatsapp_instrucciones.xlsx

    Columnas esperadas:
      - Apartamentos
      - WA ES
      - WA EN
      - WA_URL_MAPS
      - WA_YOUTUBE
      - PRIMER_CONTACTO_ES
      - PRIMER_CONTACTO_EN
      - ACTIVO
    """
    path = os.path.join("data", "whatsapp_instrucciones.xlsx")
    if not os.path.exists(path):
        return pd.DataFrame()

    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception:
        return pd.DataFrame()

    df.columns = [str(c).strip() for c in df.columns]

    ren = {}
    for c in df.columns:
        cl = str(c).strip().lower()

        if cl in {"apartamentos", "apartamento", "apartment"}:
            ren[c] = "Apartamentos"

        elif cl in {"wa es", "wa_es", "waes"}:
            ren[c] = "WA ES"
        elif cl in {"wa en", "wa_en", "waen"}:
            ren[c] = "WA EN"

        elif cl in {"wa_url_maps", "wa maps", "wa_maps", "wa url maps"}:
            ren[c] = "WA_URL_MAPS"

        elif cl in {"wa_youtube", "wa youtube", "youtube", "wa_yt"}:
            ren[c] = "WA_YOUTUBE"

        elif cl in {"primer_contacto_es", "primer contacto es", "primer_contacto (es)"}:
            ren[c] = "PRIMER_CONTACTO_ES"
        elif cl in {"primer_contacto_en", "primer contacto en", "primer_contacto (en)"}:
            ren[c] = "PRIMER_CONTACTO_EN"

        elif cl in {"activo", "active"}:
            ren[c] = "ACTIVO"

    if ren:
        df = df.rename(columns=ren)

    if "Apartamentos" not in df.columns:
        return pd.DataFrame()

    df["Apartamentos"] = df["Apartamentos"].astype(str).str.strip()
    df["APARTAMENTO_KEY"] = df["Apartamentos"].map(_apt_key)

    if "ACTIVO" not in df.columns:
        df["ACTIVO"] = 1

    for c in ["WA ES", "WA EN", "WA_URL_MAPS", "WA_YOUTUBE", "PRIMER_CONTACTO_ES", "PRIMER_CONTACTO_EN"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].apply(_safe_str)

    df["ACTIVO"] = pd.to_numeric(df["ACTIVO"], errors="coerce").fillna(0).astype(int)

    df = df[df["APARTAMENTO_KEY"].astype(str).str.strip().ne("")].copy()
    df = df.drop_duplicates(subset=["APARTAMENTO_KEY"], keep="first").reset_index(drop=True)
    return df


def add_whatsapp_links_to_df(df: pd.DataFrame, wa_master: pd.DataFrame) -> pd.DataFrame:
    """
    Añade columnas (4):
      - WA_ES_LINK
      - WA_EN_LINK
      - PRIMER_ES_LINK
      - PRIMER_EN_LINK
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    if "APARTAMENTO_KEY" not in out.columns and "APARTAMENTO" in out.columns:
        out["APARTAMENTO_KEY"] = out["APARTAMENTO"].map(_apt_key)

    if wa_master is None or wa_master.empty:
        out["WA_ES_LINK"] = ""
        out["WA_EN_LINK"] = ""
        out["PRIMER_ES_LINK"] = ""
        out["PRIMER_EN_LINK"] = ""
        return out

    wam = wa_master[wa_master["ACTIVO"].eq(1)].copy()
    keep_cols = [
        "APARTAMENTO_KEY",
        "WA ES",
        "WA EN",
        "WA_URL_MAPS",
        "WA_YOUTUBE",
        "PRIMER_CONTACTO_ES",
        "PRIMER_CONTACTO_EN",
    ]
    for c in keep_cols:
        if c not in wam.columns:
            wam[c] = ""
    wam = wam[keep_cols].copy()

    out = out.merge(wam, on="APARTAMENTO_KEY", how="left")

    def _row_es(r):
        tel = _wa_phone_digits(r.get("Teléfono", ""))
        nombre = _first_name(r.get("Cliente", ""))
        msg = _compose_wa_message(
            nombre=nombre,
            body=r.get("WA ES", ""),
            url_maps=r.get("WA_URL_MAPS", ""),
            url_youtube=r.get("WA_YOUTUBE", ""),
            lang="ES",
        )
        u = _wa_send_url(tel, msg)
        return u or ""

    def _row_en(r):
        tel = _wa_phone_digits(r.get("Teléfono", ""))
        nombre = _first_name(r.get("Cliente", ""))
        msg = _compose_wa_message(
            nombre=nombre,
            body=r.get("WA EN", ""),
            url_maps=r.get("WA_URL_MAPS", ""),
            url_youtube=r.get("WA_YOUTUBE", ""),
            lang="EN",
        )
        u = _wa_send_url(tel, msg)
        return u or ""

    def _row_p_es(r):
        tel = _wa_phone_digits(r.get("Teléfono", ""))
        nombre = _first_name(r.get("Cliente", ""))
        msg = _compose_first_contact(nombre=nombre, body=r.get("PRIMER_CONTACTO_ES", ""), lang="ES")
        u = _wa_send_url(tel, msg)
        return u or ""

    def _row_p_en(r):
        tel = _wa_phone_digits(r.get("Teléfono", ""))
        nombre = _first_name(r.get("Cliente", ""))
        msg = _compose_first_contact(nombre=nombre, body=r.get("PRIMER_CONTACTO_EN", ""), lang="EN")
        u = _wa_send_url(tel, msg)
        return u or ""

    out["WA_ES_LINK"] = out.apply(_row_es, axis=1)
    out["WA_EN_LINK"] = out.apply(_row_en, axis=1)
    out["PRIMER_ES_LINK"] = out.apply(_row_p_es, axis=1)
    out["PRIMER_EN_LINK"] = out.apply(_row_p_en, axis=1)

    return out


# =========================
# ✅ Limpieza (READY) desde Google Sheet (cruda)
# =========================
def _find_col_case_insensitive(df: pd.DataFrame, candidates: list[str]) -> str | None:
    if df is None or df.empty:
        return None
    low = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = str(cand).strip().lower()
        if key in low:
            return low[key]
    return None


def build_cleaning_master_from_sheet(sheet_df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye maestro de última limpieza por apartamento usando la sheet cruda.
    Espera columnas típicas:
      - 'Marca temporal' (timestamp)
      - 'Apartamento'
    """
    if sheet_df is None or sheet_df.empty:
        return pd.DataFrame()

    ts_col = _find_col_case_insensitive(sheet_df, ["Marca temporal", "marca temporal", "timestamp", "time stamp"])
    apt_col = _find_col_case_insensitive(sheet_df, ["Apartamento", "apartamento", "apto", "apartment"])

    if ts_col is None or apt_col is None:
        return pd.DataFrame()

    tmp = sheet_df[[ts_col, apt_col]].copy()
    tmp = tmp.rename(columns={ts_col: "TS_RAW", apt_col: "APT_RAW"})
    tmp["APARTAMENTO_KEY"] = tmp["APT_RAW"].map(_apt_key)

    # dayfirst=True por tu formato dd/mm/yyyy HH:MM:SS
    tmp["LAST_CLEAN_TS"] = pd.to_datetime(tmp["TS_RAW"], errors="coerce", dayfirst=True)
    tmp = tmp.dropna(subset=["APARTAMENTO_KEY", "LAST_CLEAN_TS"]).copy()
    tmp = tmp[tmp["APARTAMENTO_KEY"].astype(str).str.strip().ne("")].copy()

    master = (
        tmp.sort_values("LAST_CLEAN_TS")
        .groupby("APARTAMENTO_KEY", as_index=False)["LAST_CLEAN_TS"]
        .max()
        .reset_index(drop=True)
    )
    master["Última limp"] = master["LAST_CLEAN_TS"].dt.strftime("%d/%m %H:%M")
    return master


def add_cleaning_ready_columns(oper_df: pd.DataFrame, cleaning_master: pd.DataFrame, lookback_days: int = 3) -> pd.DataFrame:
    """
    Añade a oper_df:
      - '🧹' (🟢/🔴) en función de última limpieza y el día de la fila (col 'Día')
      - 'Última limp' (dd/mm HH:MM)
    Criterio (ventana):
      última_limpieza entre [Día - lookback_days, Día] (inclusive)
    """
    if oper_df is None or oper_df.empty:
        return oper_df

    out = oper_df.copy()

    if "APARTAMENTO_KEY" not in out.columns and "APARTAMENTO" in out.columns:
        out["APARTAMENTO_KEY"] = out["APARTAMENTO"].map(_apt_key)

    if cleaning_master is None or cleaning_master.empty:
        out["🧹"] = "🔴"
        out["Última limp"] = ""
        return out

    cm = cleaning_master.copy()
    cm = cm.drop_duplicates(subset=["APARTAMENTO_KEY"], keep="first")
    out = out.merge(cm[["APARTAMENTO_KEY", "LAST_CLEAN_TS", "Última limp"]], on="APARTAMENTO_KEY", how="left")

    row_day = pd.to_datetime(out.get("Día"), errors="coerce").dt.normalize()
    last_day = pd.to_datetime(out.get("LAST_CLEAN_TS"), errors="coerce").dt.normalize()

    min_day = row_day - pd.Timedelta(days=int(lookback_days))
    ready = (last_day.notna()) & (row_day.notna()) & (last_day >= min_day) & (last_day <= row_day)

    out["🧹"] = ready.map(lambda x: "🟢" if x else "🔴")
    out["Última limp"] = out["Última limp"].fillna("")

    return out


# =========================
# Google Maps helpers
# =========================
def _coord_str(lat, lng):
    try:
        return f"{float(lat):.8f},{float(lng):.8f}"
    except Exception:
        return None


def build_gmaps_directions_url(coords, travelmode="walking", return_to_base=False):
    clean = []
    seen = set()
    for c in coords:
        if isinstance(c, str) and "," in c and c not in seen:
            seen.add(c)
            clean.append(c)

    if not clean:
        return None

    origin = f"{ORIGIN_LAT:.8f},{ORIGIN_LNG:.8f}"

    if return_to_base:
        destination = origin
        waypoints = clean
    else:
        destination = clean[-1]
        waypoints = clean[:-1]

    wp = "|".join(waypoints)

    url = "https://www.google.com/maps/dir/?api=1"
    url += f"&origin={quote(origin)}"
    url += f"&destination={quote(destination)}"
    if wp:
        url += f"&waypoints={quote(wp)}"
    url += f"&travelmode={quote(travelmode)}"
    return url


def chunk_list(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


# =========================
# Styles
# =========================
def _style_operativa(df: pd.DataFrame):
    colors = {
        "ENTRADA+SALIDA": "#FFF3BF",
        "ENTRADA": "#D3F9D8",
        "SALIDA": "#FFE8CC",
        "OCUPADO": "#E7F5FF",
        "VACIO": "#F1F3F5",
    }

    def row_style(row):
        bg = colors.get(str(row.get("Estado", "")), "")
        if bg:
            return [f"background-color: {bg}"] * len(row)
        return [""] * len(row)

    return df.style.apply(row_style, axis=1)


# =========================
# Render helper
# =========================
def _render_operativa_table(df: pd.DataFrame, key: str, styled: bool = True):
    if df is None or df.empty:
        st.info("Sin resultados.")
        return

    view = df.copy()
    colcfg = {}

    # Limpieza status
    if "🧹" in view.columns:
        colcfg["🧹"] = st.column_config.TextColumn("🧹", width="small", max_chars=2)
    if "Última limp" in view.columns:
        colcfg["Última limp"] = st.column_config.TextColumn("Última limp", width="small", max_chars=50)

    # WhatsApp links
    if "PRIMER_ES_LINK" in view.columns:
        colcfg["PRIMER_ES_LINK"] = st.column_config.LinkColumn(
            "1º ES",
            help="Primer contacto (ES) con saludo + nombre",
            display_text="Abrir",
            width="small",
        )
    if "PRIMER_EN_LINK" in view.columns:
        colcfg["PRIMER_EN_LINK"] = st.column_config.LinkColumn(
            "1º EN",
            help="First contact (EN) with greeting + name",
            display_text="Open",
            width="small",
        )

    if "WA_ES_LINK" in view.columns:
        colcfg["WA_ES_LINK"] = st.column_config.LinkColumn(
            "WA ES",
            help="Instrucciones llegada (ES) con saludo + nombre + links",
            display_text="Abrir",
            width="small",
        )
    if "WA_EN_LINK" in view.columns:
        colcfg["WA_EN_LINK"] = st.column_config.LinkColumn(
            "WA EN",
            help="Arrival instructions (EN) with greeting + name + links",
            display_text="Open",
            width="small",
        )

    for c in ["Lista_reponer", "Completar con", "Producto", "Cliente"]:
        if c in view.columns:
            colcfg[c] = st.column_config.TextColumn(c, width="large", max_chars=10000)

    if "APARTAMENTO" in view.columns:
        colcfg["APARTAMENTO"] = st.column_config.TextColumn("APARTAMENTO", width="medium", max_chars=5000)

    if "Teléfono" in view.columns:
        colcfg["Teléfono"] = st.column_config.TextColumn("Teléfono", width="medium", max_chars=200)

    if styled:
        st.dataframe(_style_operativa(view), use_container_width=True, height="content", column_config=colcfg)
    else:
        st.dataframe(view, use_container_width=True, height="content", column_config=colcfg)


# =========================
# Reposición parsing
# =========================
_ITEM_RX = re.compile(r"^\s*(.*?)\s*x\s*([0-9]+)\s*$", re.IGNORECASE)


def parse_lista_reponer(s: str):
    if s is None:
        return []
    txt = str(s).strip()
    if not txt:
        return []
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    out = []
    for p in parts:
        m = _ITEM_RX.match(p)
        if m:
            name = m.group(1).strip()
            qty = int(m.group(2))
            if name:
                out.append((name, qty))
        else:
            out.append((p, 1))
    return out


def build_sugerencia_df(operativa: pd.DataFrame, zonas_sel: list[str], include_completar: bool = False):
    df = operativa.copy()
    df = df[df["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA", "VACIO"])].copy()

    if zonas_sel:
        df = df[df["ZONA"].isin(zonas_sel)].copy()

    cols = ["Lista_reponer"]
    if include_completar and "Completar con" in df.columns:
        cols.append("Completar con")

    rows = []
    for _, r in df.iterrows():
        for col in cols:
            txt = r.get(col, "")
            if str(txt).strip() == "":
                continue
            items = parse_lista_reponer(txt)
            for prod, qty in items:
                rows.append(
                    {
                        "Día": r.get("Día"),
                        "ZONA": r.get("ZONA"),
                        "APARTAMENTO": r.get("APARTAMENTO"),
                        "Producto": prod,
                        "Cantidad": int(qty),
                        "Fuente": col,
                    }
                )

    items_df = pd.DataFrame(rows)
    if items_df.empty:
        totals_df = pd.DataFrame(columns=["Producto", "Total"])
        return items_df, totals_df

    totals_df = (
        items_df.groupby("Producto", as_index=False)["Cantidad"]
        .sum()
        .rename(columns={"Cantidad": "Total"})
        .sort_values(["Total", "Producto"], ascending=[False, True])
        .reset_index(drop=True)
    )

    items_df = items_df.sort_values(["ZONA", "APARTAMENTO", "Producto", "Fuente"]).reset_index(drop=True)
    return items_df, totals_df


# =========================
# KPI table
# =========================
def _kpi_table(df: pd.DataFrame, title: str):
    if df is None or df.empty:
        st.info("Sin resultados.")
        return

    cols_pref = []
    for c in ["🧹", "Última limp", "PRIMER_ES_LINK", "PRIMER_EN_LINK", "WA_ES_LINK", "WA_EN_LINK"]:
        if c in df.columns:
            cols_pref.append(c)

    cols_show = [
        c
        for c in [
            "Día",
            "ZONA",
            "APARTAMENTO",
            "Cliente",
            "Teléfono",
            "Nº Adultos",
            "Nº Niños",
            "Hora Check-in",
            "Estado",
            "Lista_reponer",
            "Completar con",
        ]
        if c in df.columns
    ]

    st.markdown(f"#### {title}")
    view = df[cols_pref + cols_show].reset_index(drop=True)
    _render_operativa_table(view, key=f"kpi_{_apt_key(title)}", styled=False)


# =========================
# Enriquecimiento: adultos/niños/hora check-in/teléfono desde Avantio (Entradas)
# =========================
def _detect_checkin_datetime_col(avantio_df: pd.DataFrame) -> str | None:
    for c in avantio_df.columns:
        cl = str(c).lower()
        if "fecha" in cl and "entrada" in cl:
            return c
        if "check" in cl and "in" in cl:
            return c
    try:
        return _col_by_excel_letter(avantio_df, "D")
    except Exception:
        return None


def enrich_operativa_with_guest_fields(operativa_df: pd.DataFrame, avantio_df: pd.DataFrame) -> pd.DataFrame:
    out = operativa_df.copy()

    if "Nº Adultos" not in out.columns:
        out["Nº Adultos"] = 0
    if "Nº Niños" not in out.columns:
        out["Nº Niños"] = 0
    if "Hora Check-in" not in out.columns:
        out["Hora Check-in"] = "16:00"
    if "Teléfono" not in out.columns:
        out["Teléfono"] = ""

    if out is None or out.empty:
        return out
    if avantio_df is None or avantio_df.empty:
        return out

    av = avantio_df.copy()

    if "APARTAMENTO" not in av.columns and "Alojamiento" in av.columns:
        av["APARTAMENTO"] = av["Alojamiento"].astype(str).str.strip()

    if "APARTAMENTO" not in av.columns:
        return out

    av["APARTAMENTO"] = av["APARTAMENTO"].astype(str).str.strip()
    av["APARTAMENTO_KEY"] = av["APARTAMENTO"].map(_apt_key)

    try:
        col_ad = _col_by_excel_letter(av, "H")
        col_ch = _col_by_excel_letter(av, "I")
        col_ci = _col_by_excel_letter(av, "Z")
        col_tel = _col_by_excel_letter(av, "N")
    except Exception:
        return out

    dtcol = _detect_checkin_datetime_col(av)
    if dtcol is None:
        return out

    av["_CHECKIN_DT"] = pd.to_datetime(av[dtcol], errors="coerce")
    av["_CHECKIN_DATE"] = av["_CHECKIN_DT"].dt.date

    av["AV_ADULTOS"] = pd.to_numeric(av[col_ad], errors="coerce").fillna(0).astype(int)
    av["AV_NINOS"] = pd.to_numeric(av[col_ch], errors="coerce").fillna(0).astype(int)
    av["AV_CHECKIN"] = av[col_ci].apply(_parse_time_to_hhmm)
    av["AV_TEL"] = av[col_tel].apply(_clean_phone)

    av_small = (
        av.dropna(subset=["APARTAMENTO_KEY", "_CHECKIN_DATE"])
        .sort_values(["APARTAMENTO_KEY", "_CHECKIN_DATE"])
        .groupby(["APARTAMENTO_KEY", "_CHECKIN_DATE"], as_index=False)
        .agg({"AV_ADULTOS": "first", "AV_NINOS": "first", "AV_CHECKIN": "first", "AV_TEL": "first"})
        .rename(columns={"_CHECKIN_DATE": "Día"})
    )

    if "APARTAMENTO_KEY" not in out.columns:
        out["APARTAMENTO_KEY"] = out["APARTAMENTO"].map(_apt_key)

    out["Día"] = pd.to_datetime(out["Día"], errors="coerce").dt.date
    out = out.merge(av_small, on=["APARTAMENTO_KEY", "Día"], how="left")

    out["Nº Adultos"] = pd.to_numeric(out["AV_ADULTOS"], errors="coerce").fillna(out["Nº Adultos"]).fillna(0).astype(int)
    out["Nº Niños"] = pd.to_numeric(out["AV_NINOS"], errors="coerce").fillna(out["Nº Niños"]).fillna(0).astype(int)
    out["Hora Check-in"] = out["AV_CHECKIN"].fillna(out["Hora Check-in"]).apply(_parse_time_to_hhmm)
    out["Teléfono"] = out["AV_TEL"].fillna(out["Teléfono"]).fillna("")

    out = out.drop(columns=["AV_ADULTOS", "AV_NINOS", "AV_CHECKIN", "AV_TEL"], errors="ignore")
    return out


def main():
    from src.loaders import load_masters_repo
    from src.parsers import parse_avantio_entradas, parse_odoo_stock
    from src.normalize import normalize_products, summarize_replenishment
    from src.dashboard import build_dashboard_frames
    from src.gsheets import read_sheet_df

    try:
        from src.cleaning_last_report import build_last_report_view
    except Exception:
        from src.parsers.cleaning_last_report import build_last_report_view

    st.set_page_config(page_title="Florit OPS – Operativa & Reposición", layout="wide")
    st.title("Florit OPS – Parte diario (Operativa + Reposición)")

    with st.expander("📌 Cómo usar", expanded=False):
        st.markdown(
            """
**Sube 2 archivos diarios:**
- **Avantio (Entradas)**: .xls / .xlsx / .csv / (xls HTML de Avantio)
- **Odoo (stock.quant)**: .xlsx / .csv

📌 Maestros en `data/` (GitHub):
- Zonas
- Apartamentos e Inventarios (ALMACEN + Localización)
- Café por apartamento
- Stock mínimo/máximo
- whatsapp_instrucciones.xlsx (WA + 1er contacto por apartamento)
"""
        )

    st.sidebar.header("Archivos diarios")
    avantio_file = st.sidebar.file_uploader("Avantio (Entradas)", type=["xls", "xlsx", "csv", "html"])
    odoo_file = st.sidebar.file_uploader("Odoo (stock.quant)", type=["xlsx", "csv"])

    with st.sidebar.expander("Avanzado (opcional)", expanded=True):
        st.subheader("Periodo operativo")
        period_start = st.date_input("Inicio", value=pd.Timestamp.today().date())
        period_days = st.number_input("Nº días", min_value=1, max_value=14, value=2, step=1)

        st.divider()
        st.subheader("Reposición")
        mode = st.radio(
            "Modo",
            ["Reponer hasta máximo", "URGENTE: solo bajo mínimo (pero reponiendo hasta máximo)"],
            index=0,
        )

        st.divider()
        st.subheader("Filtros")
        estados_sel = st.multiselect(
            "Filtrar estados",
            ["ENTRADA", "SALIDA", "ENTRADA+SALIDA", "OCUPADO", "VACIO"],
            default=["ENTRADA", "SALIDA", "ENTRADA+SALIDA", "OCUPADO", "VACIO"],
        )

        st.divider()
        st.subheader("Ruta (HOY + MAÑANA)")
        travelmode = st.selectbox("Modo", ["walking", "driving"], index=0)
        return_to_base = st.checkbox("Volver a Florit Flats al final", value=False)

    try:
        masters = load_masters_repo()
        st.sidebar.success("Maestros cargados ✅")
    except Exception as e:
        st.error("Fallo cargando maestros (data/).")
        st.exception(e)
        st.stop()

    wa_master = load_whatsapp_master_from_data()
    if wa_master is None or wa_master.empty:
        st.sidebar.warning("WhatsApp maestro: no encontrado o vacío (data/whatsapp_instrucciones.xlsx).")
    else:
        st.sidebar.success(f"WhatsApp maestro cargado ✅ ({len(wa_master)} apts)")

    zonas_all = (
        masters["zonas"]["ZONA"].dropna().astype(str).str.strip().unique().tolist()
        if "zonas" in masters and "ZONA" in masters["zonas"].columns
        else []
    )
    zonas_all = sorted([z for z in zonas_all if z and z.lower() not in ["nan", "none"]])
    zonas_sel = st.sidebar.multiselect("ZONAS (multiselección)", options=zonas_all, default=zonas_all)

    if not (avantio_file and odoo_file):
        st.info("Sube Avantio + Odoo para generar el parte operativo.")
        st.stop()

    avantio_df = parse_avantio_entradas(avantio_file)
    odoo_df = parse_odoo_stock(odoo_file)
    if odoo_df is None or odoo_df.empty:
        st.error("Odoo: no se pudieron leer datos del stock.quant.")
        st.stop()

    if "Alojamiento" in avantio_df.columns:
        avantio_df["APARTAMENTO"] = avantio_df["Alojamiento"].astype(str).str.strip()
    elif "APARTAMENTO" in avantio_df.columns:
        avantio_df["APARTAMENTO"] = avantio_df["APARTAMENTO"].astype(str).str.strip()
    else:
        st.error("Avantio (Entradas): no encuentro columna 'Alojamiento' ni 'APARTAMENTO'.")
        st.stop()

    avantio_df["APARTAMENTO_KEY"] = avantio_df["APARTAMENTO"].map(_apt_key)

    avantio_df = avantio_df.merge(masters["zonas"], on="APARTAMENTO", how="left")
    avantio_df = avantio_df.merge(masters["cafe"], on="APARTAMENTO", how="left")

    ap_map = masters["apt_almacen"].copy()
    need = {"APARTAMENTO", "ALMACEN"}
    if not need.issubset(set(ap_map.columns)):
        st.error(f"Maestro apt_almacen: faltan columnas {need}. Columnas: {list(ap_map.columns)}")
        st.stop()

    for c in ["LAT", "LNG"]:
        if c not in ap_map.columns:
            ap_map[c] = pd.NA

    if "Localizacion" in ap_map.columns:

        def _split_loc(x):
            s = str(x).strip()
            if "," in s:
                a, b = s.split(",", 1)
                return a.strip(), b.strip()
            return None, None

        miss = ap_map["LAT"].isna() | ap_map["LNG"].isna()
        if miss.any():
            loc_pairs = ap_map.loc[miss, "Localizacion"].apply(_split_loc)
            ap_map.loc[miss, "LAT"] = [p[0] for p in loc_pairs]
            ap_map.loc[miss, "LNG"] = [p[1] for p in loc_pairs]

    ap_map = ap_map[["APARTAMENTO", "ALMACEN", "LAT", "LNG"]].dropna(subset=["APARTAMENTO", "ALMACEN"]).drop_duplicates()
    ap_map["APARTAMENTO"] = ap_map["APARTAMENTO"].astype(str).str.strip()
    ap_map["ALMACEN"] = ap_map["ALMACEN"].astype(str).str.strip()

    avantio_df = avantio_df.merge(ap_map, on="APARTAMENTO", how="left")

    odoo_norm = normalize_products(odoo_df)
    if "Ubicación" in odoo_norm.columns:
        odoo_norm = odoo_norm.rename(columns={"Ubicación": "ALMACEN"})
    odoo_norm["ALMACEN"] = odoo_norm["ALMACEN"].astype(str).str.strip()

    stock_by_alm = (
        odoo_norm.groupby(["ALMACEN", "AmenityKey"], as_index=False)["Cantidad"]
        .sum()
        .rename(columns={"Cantidad": "Cantidad"})
    )

    urgent_only = mode.startswith("URGENTE")
    rep_all = summarize_replenishment(stock_by_alm, masters["thresholds"], objective="max", urgent_only=False)
    rep = summarize_replenishment(stock_by_alm, masters["thresholds"], objective="max", urgent_only=urgent_only)

    unclassified = odoo_norm[odoo_norm["AmenityKey"].isna()][["ALMACEN", "Producto", "Cantidad"]].copy()

    dash = build_dashboard_frames(
        avantio_df=avantio_df,
        replenishment_df=rep,
        rep_all_df=rep_all,
        urgent_only=urgent_only,
        unclassified_products=unclassified,
        period_start=period_start,
        period_days=period_days,
    )

    # =========================
    # ✅ Cargar limpieza desde Google Sheet (cruda) y crear maestro de última limpieza
    # =========================
    sheet_df = None
    last_view = pd.DataFrame()
    cleaning_master = pd.DataFrame()

    try:
        sheet_df = read_sheet_df()
        if sheet_df is not None and not sheet_df.empty:
            cleaning_master = build_cleaning_master_from_sheet(sheet_df)

            # Vista bonita (la que ya usabas en buscador)
            last_view = build_last_report_view(sheet_df)
            if last_view is not None and not last_view.empty and "Apartamento" in last_view.columns:
                last_view["APARTAMENTO_KEY"] = last_view["Apartamento"].map(_apt_key)
    except Exception as e:
        st.warning("No pude leer / procesar la Sheet de limpieza.")
        st.exception(e)

    # =========================
    # ✅ DASHBOARD ARRIBA + "CLICK" para ver listados
    # =========================
    if "kpi_open" not in st.session_state:
        st.session_state["kpi_open"] = ""

    tz = ZoneInfo("Europe/Madrid")
    today_real = pd.Timestamp.now(tz=tz).normalize().date()
    foco_day = pd.Timestamp(dash.get("period_start")).normalize().date()

    oper_all = dash["operativa"].copy()
    oper_all["APARTAMENTO_KEY"] = oper_all["APARTAMENTO"].map(_apt_key)

    oper_all = enrich_operativa_with_guest_fields(oper_all, avantio_df)
    oper_all = add_whatsapp_links_to_df(oper_all, wa_master)

    # ✅ Añade 🧹 + Última limp usando ventana de 3 días
    oper_all = add_cleaning_ready_columns(oper_all, cleaning_master, lookback_days=CLEAN_READY_LOOKBACK_DAYS)

    oper_foco = oper_all[oper_all["Día"] == foco_day].copy()

    presencial_set = {"APOLO 029", "APOLO 180", "APOLO 197", "SERRANOS"}
    presencial_keys = {_apt_key(x) for x in presencial_set}

    pres_df = oper_all[
        (oper_all["Día"] == foco_day)
        & (oper_all["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA"]))
        & (oper_all["APARTAMENTO_KEY"].isin(presencial_keys))
    ].copy()

    pres_label = "HOY" if foco_day == today_real else pd.to_datetime(foco_day).strftime("%d/%m/%Y")

    kpis = dash.get("kpis", {})
    st.divider()
    st.subheader("📊 Dashboard (día foco)")

    c1, c2, c3, c4, c5, c6 = st.columns(6)

    with c1:
        st.metric("Entradas (día foco)", kpis.get("entradas_dia", 0))
        if st.button("Ver entradas", key="kpi_btn_entradas"):
            st.session_state["kpi_open"] = "entradas"

    with c2:
        st.metric("Salidas (día foco)", kpis.get("salidas_dia", 0))
        if st.button("Ver salidas", key="kpi_btn_salidas"):
            st.session_state["kpi_open"] = "salidas"

    with c3:
        st.metric("Turnovers", kpis.get("turnovers_dia", 0))
        if st.button("Ver turnovers", key="kpi_btn_turnovers"):
            st.session_state["kpi_open"] = "turnovers"

    with c4:
        st.metric("Ocupados", kpis.get("ocupados_dia", 0))
        if st.button("Ver ocupados", key="kpi_btn_ocupados"):
            st.session_state["kpi_open"] = "ocupados"

    with c5:
        st.metric("Vacíos", kpis.get("vacios_dia", 0))
        if st.button("Ver vacíos", key="kpi_btn_vacios"):
            st.session_state["kpi_open"] = "vacios"

    with c6:
        st.metric(f"Check-ins presenciales ({pres_label})", int(len(pres_df)))
        if st.button("Ver presenciales", key="kpi_btn_presenciales"):
            st.session_state["kpi_open"] = "presenciales"

    kpi_open = st.session_state.get("kpi_open", "")
    if kpi_open:
        st.divider()
        st.subheader("📌 Detalle KPI")

        if kpi_open == "entradas":
            df = oper_foco[oper_foco["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA"])].copy()
            _kpi_table(df, f"Entradas · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")

        elif kpi_open == "salidas":
            df = oper_foco[oper_foco["Estado"].isin(["SALIDA", "ENTRADA+SALIDA"])].copy()
            _kpi_table(df, f"Salidas · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")

        elif kpi_open == "turnovers":
            df = oper_foco[oper_foco["Estado"].isin(["ENTRADA+SALIDA"])].copy()
            _kpi_table(df, f"Turnovers · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")

        elif kpi_open == "ocupados":
            df = oper_foco[oper_foco["Estado"].isin(["OCUPADO"])].copy()
            _kpi_table(df, f"Ocupados · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")

        elif kpi_open == "vacios":
            df = oper_foco[oper_foco["Estado"].isin(["VACIO"])].copy()
            _kpi_table(df, f"Vacíos · {pd.to_datetime(foco_day).strftime('%d/%m/%Y')}")

        elif kpi_open == "presenciales":
            _kpi_table(pres_df, f"Check-ins presenciales · {pres_label}")

        st.caption("Para cerrar, pulsa otro KPI o recarga la página.")

    # =========================
    # 🔎 BUSCADOR PRINCIPAL (MULTISELECT)
    # =========================
    st.divider()
    st.subheader("🔎 Buscar apartamento · Resumen (Limpieza + Operativa + Reposición)")
    st.caption("Selecciona uno o varios apartamentos del listado (es buscable).")

    apt_options = []
    try:
        apt_options = (
            masters["apt_almacen"]["APARTAMENTO"].dropna().astype(str).str.strip().tolist()
            if "apt_almacen" in masters and "APARTAMENTO" in masters["apt_almacen"].columns
            else []
        )
    except Exception:
        apt_options = []

    apt_options = sorted([a for a in apt_options if a and a.lower() not in {"nan", "none"}])

    if "apt_selected" not in st.session_state:
        st.session_state["apt_selected"] = []

    selected_apts = st.multiselect(
        "Apartamentos",
        options=apt_options,
        default=st.session_state["apt_selected"],
        key="apt_selected",
        placeholder="Escribe para buscar…",
    )

    apt_keys_sel = [_apt_key(a) for a in selected_apts]

    if apt_keys_sel:
        st.markdown("### 🧹 Última limpieza (según Marca temporal)")
        if last_view is None or last_view.empty:
            st.info("No hay datos de limpieza disponibles.")
        else:
            one = last_view[last_view["APARTAMENTO_KEY"].isin(apt_keys_sel)].copy()
            if one.empty:
                st.info("No encuentro último informe para esos apartamentos en la Sheet.")
            else:
                show_cols = ["Apartamento", "Último informe", "LLAVES", "OTRAS REPOSICIONES", "INCIDENCIAS/TAREAS A REALIZAR"]
                show_cols = [c for c in show_cols if c in one.columns]
                st.dataframe(one[show_cols].reset_index(drop=True), use_container_width=True, height="content")

        st.markdown("### 🧾 Parte Operativo (apartamentos seleccionados)")
        op_one = oper_all[oper_all["APARTAMENTO_KEY"].isin(apt_keys_sel)].copy()
        if op_one.empty:
            st.info("No hay filas de operativa para esos apartamentos en el periodo seleccionado.")
        else:
            if zonas_sel:
                op_one = op_one[op_one["ZONA"].isin(zonas_sel)].copy()
            if estados_sel:
                op_one = op_one[op_one["Estado"].isin(estados_sel)].copy()

            op_one = op_one.sort_values(["Día", "ZONA", "__prio", "APARTAMENTO"], ascending=[True, True, True, True])
            op_show = op_one.drop(columns=["APARTAMENTO_KEY"], errors="ignore").copy()
            _render_operativa_table(op_show, key="apt_oper_multiselect", styled=True)

        st.markdown("### 📦 Reposición (apartamentos seleccionados)")
        if op_one.empty:
            st.info("Sin reposición (no hay operativa para esos apartamentos).")
        else:
            cols_rep = [c for c in ["Lista_reponer", "Completar con"] if c in op_one.columns]
            rep_rows = op_one[cols_rep + ["Día", "ZONA", "APARTAMENTO"]].copy() if cols_rep else pd.DataFrame()
            if rep_rows.empty:
                st.info("No veo columnas de reposición en la operativa para esos apartamentos.")
            else:
                rep_rows["has_rep"] = rep_rows[cols_rep].astype(str).apply(
                    lambda r: any(x.strip().lower() not in {"", "nan", "none"} for x in r),
                    axis=1,
                )
                rep_rows = rep_rows[rep_rows["has_rep"]].drop(columns=["has_rep"], errors="ignore")
                if rep_rows.empty:
                    st.info("No hay reposición indicada para esos apartamentos en el periodo.")
                else:
                    _render_operativa_table(rep_rows.reset_index(drop=True), key="apt_rep_multiselect", styled=False)
    else:
        st.caption("Selecciona uno o varios apartamentos para ver el resumen.")

    st.download_button(
        "⬇️ Descargar Excel (Operativa)",
        data=dash["excel_all"],
        file_name=dash["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # =========================
    # PARTE OPERATIVO COMPLETO
    # =========================
    st.divider()
    st.subheader("PARTE OPERATIVO · Entradas / Salidas / Ocupación / Vacíos + Reposición")
    st.caption(f"Periodo: {dash['period_start']} → {dash['period_end']} · Prioridad: Entradas arriba · Agrupado por ZONA")

    operativa = dash["operativa"].copy()
    operativa["APARTAMENTO_KEY"] = operativa["APARTAMENTO"].map(_apt_key)
    operativa = enrich_operativa_with_guest_fields(operativa, avantio_df)
    operativa = add_whatsapp_links_to_df(operativa, wa_master)
    operativa = add_cleaning_ready_columns(operativa, cleaning_master, lookback_days=CLEAN_READY_LOOKBACK_DAYS)

    if zonas_sel:
        operativa = operativa[operativa["ZONA"].isin(zonas_sel)].copy()
    if estados_sel:
        operativa = operativa[operativa["Estado"].isin(estados_sel)].copy()

    operativa = operativa.sort_values(["Día", "ZONA", "__prio", "APARTAMENTO"])

    for dia, ddf in operativa.groupby("Día", dropna=False):
        st.markdown(f"### Día {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
        if ddf.empty:
            st.info("Sin datos.")
            continue

        for zona, zdf in ddf.groupby("ZONA", dropna=False):
            zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
            st.markdown(f"#### {zona_label}")
            show_df = zdf.drop(columns=["ZONA", "__prio", "APARTAMENTO_KEY"], errors="ignore").copy()
            _render_operativa_table(
                show_df,
                key=f"oper_{pd.to_datetime(dia).strftime('%Y%m%d')}_{_apt_key(str(zona_label))}",
                styled=True,
            )

    # =========================
    # SUGERENCIA DE REPOSICIÓN
    # =========================
    st.divider()
    st.subheader("Sugerencia de Reposición")

    if urgent_only:
        st.caption("Modo URGENTE: Totales + dónde dejar, incluyendo Lista_reponer (urgente) y Completar con.")
        items_df, totals_df = build_sugerencia_df(dash["operativa"], zonas_sel, include_completar=True)
    else:
        st.caption("Resumen del periodo: ENTRADA / ENTRADA+SALIDA / VACIO con reposición. Totales + dónde dejar.")
        items_df, totals_df = build_sugerencia_df(dash["operativa"], zonas_sel, include_completar=False)

    if items_df.empty:
        st.info("No hay reposición sugerida para el periodo (con esos criterios) o faltan listas.")
    else:
        colA, colB = st.columns([1, 2])
        with colA:
            st.markdown("**Totales (preparar carrito)**")
            st.dataframe(totals_df, use_container_width=True, height="content")
        with colB:
            st.markdown("**Dónde dejar cada producto** (por ZONA y APARTAMENTO)")
            _render_operativa_table(items_df, key="sugerencia_items", styled=False)

    # =========================
    # RUTAS GOOGLE MAPS
    # =========================
    st.divider()
    st.subheader("📍 Ruta Google Maps · Reposición HOY + MAÑANA (por ZONA)")
    st.caption("Criterio: con reposición y Estado == VACIO o ENTRADA o ENTRADA+SALIDA ese día. Botones directos a Maps.")

    tomorrow = (pd.Timestamp(today_real) + pd.Timedelta(days=1)).date()
    visitable_states = {"VACIO", "ENTRADA", "ENTRADA+SALIDA"}

    route_df = dash["operativa"].copy()
    route_df = route_df[route_df["Día"].isin([today_real, tomorrow])].copy()
    route_df = route_df[route_df["Estado"].isin(visitable_states)].copy()
    route_df = route_df[route_df["Lista_reponer"].astype(str).str.strip().ne("")].copy()

    if zonas_sel:
        route_df = route_df[route_df["ZONA"].isin(zonas_sel)].copy()

    route_df = route_df.merge(ap_map[["APARTAMENTO", "LAT", "LNG"]], on="APARTAMENTO", how="left")
    route_df["COORD"] = route_df.apply(lambda r: _coord_str(r.get("LAT"), r.get("LNG")), axis=1)
    route_df = route_df[route_df["COORD"].notna()].copy()

    if route_df.empty:
        st.info("No hay apartamentos visitables con reposición para HOY/MAÑANA (o faltan coordenadas).")
    else:
        MAX_STOPS = 20
        for dia, ddf in route_df.groupby("Día", dropna=False):
            st.markdown(f"### {pd.to_datetime(dia).strftime('%d/%m/%Y')}")
            for zona, zdf in ddf.groupby("ZONA", dropna=False):
                zona_label = zona if zona not in [None, "None", "", "nan"] else "Sin zona"
                coords = zdf["COORD"].tolist()
                if not coords:
                    st.info(f"{zona_label}: sin coordenadas suficientes.")
                    continue

                for idx, chunk in enumerate(chunk_list(coords, MAX_STOPS), start=1):
                    url = build_gmaps_directions_url(chunk, travelmode=travelmode, return_to_base=return_to_base)
                    if url:
                        st.link_button(f"Abrir ruta · {zona_label} (tramo {idx})", url)

    with st.expander("🧪 Debug reposición (por almacén)", expanded=False):
        st.caption("Comprueba Min/Max/Stock y el cálculo final.")
        st.dataframe(
            rep.sort_values(["ALMACEN", "Amenity"], na_position="last").reset_index(drop=True),
            use_container_width=True,
            height="content",
        )
        if not unclassified.empty:
            st.warning("Hay productos sin clasificar (no entran en reposición).")
            st.dataframe(unclassified.reset_index(drop=True), use_container_width=True, height="content")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.title("⚠️ Error en la app (detalle visible)")
        st.exception(e)
