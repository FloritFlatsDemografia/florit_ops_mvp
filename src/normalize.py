def summarize_replenishment(stock_by_alm: pd.DataFrame, thresholds: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve por ALMACEN + Amenity:
      - Cantidad (stock actual)
      - Minimo / Maximo
      - Faltan_para_min (urgente)
      - A_reponer (para llegar a máximo)
      - Bajo_minimo (flag)
    Soporta thresholds con o sin ALMACEN:
      - Si thresholds trae ALMACEN, mergea por ALMACEN + AmenityKey
      - Si no, mergea solo por AmenityKey
    """
    out = stock_by_alm.copy()
    thr = thresholds.copy()

    # --- Normaliza nombres de columnas típicos ---
    # ALMACEN
    if "Almacen" in thr.columns and "ALMACEN" not in thr.columns:
        thr = thr.rename(columns={"Almacen": "ALMACEN"})
    if "Ubicación" in thr.columns and "ALMACEN" not in thr.columns:
        thr = thr.rename(columns={"Ubicación": "ALMACEN"})
    if "Ubicacion" in thr.columns and "ALMACEN" not in thr.columns:
        thr = thr.rename(columns={"Ubicacion": "ALMACEN"})

    # Min/Max
    if "MINIMO" in thr.columns and "Minimo" not in thr.columns:
        thr = thr.rename(columns={"MINIMO": "Minimo"})
    if "MAXIMO" in thr.columns and "Maximo" not in thr.columns:
        thr = thr.rename(columns={"MAXIMO": "Maximo"})
    if "Mínimo" in thr.columns and "Minimo" not in thr.columns:
        thr = thr.rename(columns={"Mínimo": "Minimo"})
    if "Máximo" in thr.columns and "Maximo" not in thr.columns:
        thr = thr.rename(columns={"Máximo": "Maximo"})

    # --- Claves de amenity (robusto) ---
    # stock_by_alm puede venir con AmenityKey o Amenity
    if "AmenityKey" not in out.columns:
        if "Amenity" in out.columns:
            out["AmenityKey"] = out["Amenity"].astype(str).apply(_norm_txt)
        else:
            out["AmenityKey"] = None

    # thresholds puede venir con AmenityKey o Amenity
    if "AmenityKey" not in thr.columns:
        if "Amenity" in thr.columns:
            thr["AmenityKey"] = thr["Amenity"].astype(str).apply(_norm_txt)
        else:
            thr["AmenityKey"] = None

    # Asegura columnas numéricas
    if "Cantidad" not in out.columns:
        out["Cantidad"] = 0
    out["Cantidad"] = pd.to_numeric(out["Cantidad"], errors="coerce").fillna(0)

    if "Minimo" not in thr.columns:
        thr["Minimo"] = 0
    if "Maximo" not in thr.columns:
        thr["Maximo"] = 0
    thr["Minimo"] = pd.to_numeric(thr["Minimo"], errors="coerce").fillna(0)
    thr["Maximo"] = pd.to_numeric(thr["Maximo"], errors="coerce").fillna(0)

    # --- Merge: por ALMACEN si existe en thresholds ---
    merge_cols = ["AmenityKey"]
    if "ALMACEN" in thr.columns and "ALMACEN" in out.columns:
        merge_cols = ["ALMACEN", "AmenityKey"]

    out = out.merge(
        thr[merge_cols + ["Minimo", "Maximo"]].drop_duplicates(),
        on=merge_cols,
        how="left",
    )

    out["Minimo"] = out["Minimo"].fillna(0)
    out["Maximo"] = out["Maximo"].fillna(0)

    # --- Cálculos ---
    out["Faltan_para_min"] = (out["Minimo"] - out["Cantidad"]).clip(lower=0)
    out["A_reponer"] = (out["Maximo"] - out["Cantidad"]).clip(lower=0)
    out["Bajo_minimo"] = out["Faltan_para_min"] > 0

    return out
