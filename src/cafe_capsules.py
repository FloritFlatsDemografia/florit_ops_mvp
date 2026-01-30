import pandas as pd

# Placeholder: if later you want per-apartment capsule type thresholds,
# we can build them here (e.g., only show Nespresso for apartments whose CAFE_TIPO = Nespresso).
DEFAULT_CAFE_CAPSULE_RULES = pd.DataFrame([
    {"CAFE_TIPO":"Nespresso", "Amenity":"Cápsulas Nespresso"},
    {"CAFE_TIPO":"Tassimo", "Amenity":"Cápsulas Tassimo"},
    {"CAFE_TIPO":"Dolce Gusto", "Amenity":"Cápsulas Dolce Gusto"},
    {"CAFE_TIPO":"Senseo", "Amenity":"Cápsulas Senseo"},
])
