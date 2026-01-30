import pandas as pd

# Default min/max per amenity.
# Ajusta estos valores según tu operación. Puedes convertirlo a Excel luego si quieres.
DEFAULT_THRESHOLDS = pd.DataFrame([
    {"Amenity":"Azúcar", "Minimo": 10, "Maximo": 30},
    {"Amenity":"Té/Infusión", "Minimo": 10, "Maximo": 30},
    {"Amenity":"Insecticida", "Minimo": 1, "Maximo": 3},
    {"Amenity":"Gel de ducha", "Minimo": 2, "Maximo": 6},
    {"Amenity":"Champú", "Minimo": 2, "Maximo": 6},
    {"Amenity":"Escoba", "Minimo": 0, "Maximo": 1},
    {"Amenity":"Mocho/Fregona", "Minimo": 0, "Maximo": 1},
    {"Amenity":"Detergente", "Minimo": 2, "Maximo": 6},
    {"Amenity":"Jabón de manos", "Minimo": 2, "Maximo": 6},
    {"Amenity":"Vinagre", "Minimo": 1, "Maximo": 3},
    {"Amenity":"Abrillantador", "Minimo": 1, "Maximo": 3},
    {"Amenity":"Sal lavavajillas", "Minimo": 1, "Maximo": 3},
    {"Amenity":"Cápsulas Nespresso", "Minimo": 20, "Maximo": 60},
    {"Amenity":"Cápsulas Tassimo", "Minimo": 20, "Maximo": 60},
    {"Amenity":"Cápsulas Dolce Gusto", "Minimo": 20, "Maximo": 60},
    {"Amenity":"Cápsulas Senseo", "Minimo": 20, "Maximo": 60},
])
