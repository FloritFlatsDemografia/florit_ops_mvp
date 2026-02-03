import pandas as pd
from io import BytesIO
import math
from zoneinfo import ZoneInfo
from datetime import date


COFFEE_AMENITIES = {
    "Café molido",
    "Cápsulas Nespresso",
    "Cápsulas Tassimo",
    "Cápsulas Dolce Gusto",
    "Cápsulas Senseo",
}


def _safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, float) and math.isnan(x):
            return ""
    except Exception:
        pass
    return str(x)


def _allowed_coffee_amenity(cafe_tipo) -> str | None:
    t = _safe_str(cafe_tipo).strip().lower()
    if t == "":
        return None
    if "molido" in t:
        return "Café molido"
    if "tassimo" in t:
        return "Cápsulas Tassimo"
    if "dolce" in t or "gusto" in t:
        return "Cápsulas Dolce Gusto"
    if "senseo" in t:
        return "Cápsulas Senseo"
    if "nespresso" in t or "colombia" in t:
        return "Cápsulas Nespresso"
    return None


def build_dashboard_frames(
    avantio_df: pd.DataFrame,
    replenishment_df: pd.DataFrame,
    unclassified_products: pd.DataFrame | None = None,
    period_start: date | None = None,
    period_days: int = 2,
) -> dict:
    """
    BLOQUE ÚNICO OPERATIVO (por día en el periodo):
      - Seleccionas inicio + nº días
      - Para cada día, por apartamento: ENTRADA / SALIDA / ENTRADA+SALIDA / OCUPADO / VACIO
      - Priorizado por ENTRADAS y agrupable por ZONA en UI
      - Incluye Lista_reponer + Próxima Entrada

    Reglas de estado para un día d:
      - ENTRADA si existe reserva con entrada_d == d
      - SALIDA si existe reserva con salida_d == d
      - OCUPADO si existe reserva con entrada_d <= d < salida_d (pernocta)
      - VACIO si no hay solape esa noche y no hay entrada/salida ese día
      - ENTRADA+SALIDA si ambas
    """

    df = avantio_df.copy()

    # --- Hoy (Europe/Madrid) ---
    tz = ZoneInfo("Europe/Madrid")
    today = pd.Timestamp.now(tz=tz).normalize().date()

    # Periodo (para operar)
    if period_start is None:
        period_start = today
    period_days = int(period_days or 1)
    if period_days < 1:
        period_days = 1
    period_end = (pd.Timestamp(period_start) + pd.Timedelta(days=period_days - 1)).date()

    # --- Parse fechas/hora ---
    entrada_dt = pd.to_datetime(df.get("Fecha entrada hora"), errors="coerce", dayfirst=True)
    salida_dt = pd.to_datetime(df.get("Fecha salida hora"), errors="coerce", dayfirst=True)

    df["entrada_dt"] = entrada_dt
    df["salida_dt"] = salida_dt
    df["entrada_d"] = entrada_dt.dt.date
    df["salida_d"] = salida_dt.dt.date

    # ---------------------------------------------------------
    # Próxima entrada FUTURA por apartamento (mínima entrada_d > today)
    # ---------------------------------------------------------
    next_entry = (
        df[df["entrada_d"] > today]
        .groupby("APARTAMENTO", as_index=False)["entrada_d"]
        .min()
        .rename(columns={"entrada_d": "Próxima Entrada"})
    )

    # ---------------------------------------------------------
    # Lista_reponer por ALMACEN (filtrando café por CAFE_TIPO)
    # ---------------------------------------------------------
    rep = replenishment_df.copy()

    if "ALMACEN" in df.columns and "ALMACEN" in rep.columns:
        rep = rep.merge(
            df[["ALMACEN", "CAFE_TIPO"]].drop_duplicates(),
            on="ALMACEN",
            how="left",
        )

    if "Amenity" in rep.columns and "A_reponer" in rep.columns:

        def keep_row(r):
            amen = r.get("Amenity")
            if amen not in COFFEE_AMENITIES:
                return True
            allowed = _allowed_coffee_amenity(r.get("CAFE_TIPO"))
            if allowed is None:
                return True
            return amen == allowed

        rep = rep[rep.apply(keep_row, axis=1)].copy()
        rep_items = rep[rep["A_reponer"] > 0].copy()
    else:
        rep_items = pd.DataFrame(columns=["ALMACEN", "Amenity", "A_reponer"])

    if not rep_items.empty:
        rep_items["linea"] = (
            rep_items["Amenity"].astype(str)
            + " x"
            + rep_items["A_reponer"].round(0).astype(int).astype(str)
        )

        rep_items_agg = (
            rep_items.groupby("ALMACEN")["linea"]
            .apply(lambda s: ", ".join(s.tolist()[:60]))
            .reset_index()
            .rename(columns={"linea": "Lista_reponer"})
        )
    else:
        rep_items_agg = pd.DataFrame(columns=["ALMACEN", "Lista_reponer"])

    if "ALMACEN" in df.columns:
        df = df.merge(rep_items_agg, on="ALMACEN", how="left")
    else:
        df["Lista_reponer"] = ""

    df["Lista_reponer"] = df["Lista_reponer"].fillna("")

    # ---------------------------------------------------------
    # Base de apartamentos (1 fila por APARTAMENTO con sus atributos)
    # ---------------------------------------------------------
    base_cols = ["APARTAMENTO"]
    for c in ["ZONA", "CAFE_TIPO", "ALMACEN", "Lista_reponer"]:
        if c in df.columns:
            base_cols.append(c)

    base_ap = df[base_cols].drop_duplicates("APARTAMENTO").copy()
    if "ZONA" not in base_ap.columns:
        base_ap["ZONA"] = "Sin zona"
    base_ap["ZONA"] = base_ap["ZONA"].fillna("Sin zona")

    # Añadir próxima entrada
    base_ap = base_ap.merge(next_entry, on="APARTAMENTO", how="left")

    # ---------------------------------------------------------
    # Construir OPERATIVA por día en el periodo
    # ---------------------------------------------------------
    days = [
        (pd.Timestamp(period_start) + pd.Timedelta(days=i)).date()
        for i in range((pd.Timestamp(period_end) - pd.Timestamp(period_start)).days + 1)
    ]

    operativa_rows = []

    for d in days:
        # Entradas del día (hora mínima por apt)
        ent = df[df["entrada_d"] == d].copy()
        ent_h = (
            ent.groupby("APARTAMENTO", as_index=False)["entrada_dt"]
            .min()
            .rename(columns={"entrada_dt": "Entrada hora"})
        )

        # Salidas del día (hora mínima por apt)
        sal = df[df["salida_d"] == d].copy()
        sal_h = (
            sal.groupby("APARTAMENTO", as_index=False)["salida_dt"]
            .min()
            .rename(columns={"salida_dt": "Salida hora"})
        )

        # Ocupado esa noche: entrada_d <= d < salida_d (si hay cualquier reserva que cumpla)
        occ = df[(df["entrada_d"] <= d) & (df["salida_d"] > d)]
        occ_flag = occ.groupby("APARTAMENTO", as_index=False).size()[["APARTAMENTO"]]
        occ_flag["Ocupado"] = True

        day_df = base_ap.copy()
        day_df["Día"] = d

        day_df = day_df.merge(ent_h, on="APARTAMENTO", how="left")
        day_df = day_df.merge(sal_h, on="APARTAMENTO", how="left")
        day_df = day_df.merge(occ_flag, on="APARTAMENTO", how="left")

        day_df["Ocupado"] = day_df["Ocupado"].fillna(False)

        has_entry = day_df["Entrada hora"].notna()
        has_exit = day_df["Salida hora"].notna()

        # Estado operativo
        day_df["Estado"] = "VACIO"
        day_df.loc[day_df["Ocupado"], "Estado"] = "OCUPADO"
        day_df.loc[has_exit, "Estado"] = "SALIDA"
        day_df.loc[has_entry, "Estado"] = "ENTRADA"
        day_df.loc[has_entry & has_exit, "Estado"] = "ENTRADA+SALIDA"

        # Orden de prioridad dentro de zona
        prio_map = {
            "ENTRADA+SALIDA": 0,
            "ENTRADA": 1,
            "SALIDA": 2,
            "OCUPADO": 3,
            "VACIO": 4,
        }
        day_df["__prio"] = day_df["Estado"].map(prio_map).fillna(9).astype(int)

        # Solo nos interesan especialmente los que tienen reposición
        # (pero NO filtramos aquí; en UI puedes elegir mostrar todo o solo con reposición)
        operativa_rows.append(day_df)

    operativa = pd.concat(operativa_rows, ignore_index=True)

    # Limpieza/orden de columnas
    col_order = ["Día", "ZONA", "APARTAMENTO", "Estado"]
    if "Entrada hora" in operativa.columns:
        col_order.append("Entrada hora")
    if "Salida hora" in operativa.columns:
        col_order.append("Salida hora")
    if "CAFE_TIPO" in operativa.columns:
        col_order.append("CAFE_TIPO")
    if "Próxima Entrada" in operativa.columns:
        col_order.append("Próxima Entrada")
    if "Lista_reponer" in operativa.columns:
        col_order.append("Lista_reponer")

    # Asegurar solo columnas existentes
    col_order = [c for c in col_order if c in operativa.columns] + ["__prio"]
    operativa = operativa[col_order].copy()

    # ---------------------------------------------------------
    # KPIs del día foco (period_start)
    # ---------------------------------------------------------
    foco = operativa[operativa["Día"] == period_start].copy()

    kpis = {
        "dia_foco": period_start.isoformat(),
        "entradas_dia": int((foco["Estado"].isin(["ENTRADA", "ENTRADA+SALIDA"])).sum()),
        "salidas_dia": int((foco["Estado"].isin(["SALIDA", "ENTRADA+SALIDA"])).sum()),
        "turnovers_dia": int((foco["Estado"] == "ENTRADA+SALIDA").sum()),
        "ocupados_dia": int((foco["Estado"] == "OCUPADO").sum()),
        "vacios_dia": int((foco["Estado"] == "VACIO").sum()),
        "con_reposicion_dia": int((foco.get("Lista_reponer", "").astype(str).str.strip().ne("")).sum()),
    }

    # ---------------------------------------------------------
    # Excel export
    # ---------------------------------------------------------
    filename = f"FloritOPS_{today.isoformat()}.xlsx"
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        # Operativa completa (periodo)
        operativa.drop(columns=["__prio"], errors="ignore").to_excel(writer, index=False, sheet_name="Operativa")
        # Día foco
        foco.drop(columns=["__prio"], errors="ignore").to_excel(writer, index=False, sheet_name="Operativa_DiaFoco")

    return {
        "kpis": kpis,
        "operativa": operativa,  # incluye __prio para ordenar
        "excel_all": bio.getvalue(),
        "excel_filename": filename,
        "period_start": period_start,
        "period_end": period_end,
    }
