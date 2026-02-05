# ---------------------------------------------------------
# Listas por ALMACEN:
#   - Lista_reponer      -> para llegar a MAXIMO (A_reponer)
#   - Bajo_minimo        -> lo urgente para llegar a MINIMO (Faltan_para_min)
# ---------------------------------------------------------

rep = replenishment_df.copy()

# Si falta ALMACEN en rep, crea columnas vacías para no romper
if "ALMACEN" not in rep.columns:
    rep["ALMACEN"] = ""

# Asegura columnas
for c in ["Amenity", "A_reponer", "Faltan_para_min"]:
    if c not in rep.columns:
        rep[c] = 0 if c in ["A_reponer", "Faltan_para_min"] else ""

rep["A_reponer"] = pd.to_numeric(rep["A_reponer"], errors="coerce").fillna(0)
rep["Faltan_para_min"] = pd.to_numeric(rep["Faltan_para_min"], errors="coerce").fillna(0)

# (Opcional) si ya haces filtro de café por CAFE_TIPO/ALMACEN, déjalo como estaba.
# Si no, aquí no tocamos nada.

# --- Para máximo ---
rep_max = rep[rep["A_reponer"] > 0].copy()
if not rep_max.empty:
    rep_max["linea"] = rep_max["Amenity"].astype(str) + " x" + rep_max["A_reponer"].round(0).astype(int).astype(str)
    rep_max_agg = (
        rep_max.groupby("ALMACEN")["linea"]
        .apply(lambda s: ", ".join(s.tolist()[:80]))
        .reset_index()
        .rename(columns={"linea": "Lista_reponer"})
    )
else:
    rep_max_agg = pd.DataFrame(columns=["ALMACEN", "Lista_reponer"])

# --- Urgente: para mínimo ---
rep_min = rep[rep["Faltan_para_min"] > 0].copy()
if not rep_min.empty:
    rep_min["linea"] = rep_min["Amenity"].astype(str) + " x" + rep_min["Faltan_para_min"].round(0).astype(int).astype(str)
    rep_min_agg = (
        rep_min.groupby("ALMACEN")["linea"]
        .apply(lambda s: ", ".join(s.tolist()[:80]))
        .reset_index()
        .rename(columns={"linea": "Bajo_minimo"})
    )
else:
    rep_min_agg = pd.DataFrame(columns=["ALMACEN", "Bajo_minimo"])

# Merge a df (tu df base de apartamentos)
df = df.merge(rep_max_agg, on="ALMACEN", how="left")
df = df.merge(rep_min_agg, on="ALMACEN", how="left")

df["Lista_reponer"] = df["Lista_reponer"].fillna("")
df["Bajo_minimo"] = df["Bajo_minimo"].fillna("")
