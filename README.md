# Florit OPS – Streamlit (MVP)

## Qué hace
- Subes 2 archivos diarios: Avantio (Entradas) + Odoo (stock.quant)
- Cruza apartamentos ↔ almacenes
- Normaliza productos Odoo a amenities genéricos (incluye cápsulas de café)
- Calcula faltantes (min) y reposición hasta máximo (max)
- Dashboard con 3 bloques operativos

## Ejecutar local
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Deploy (Streamlit Community Cloud)
- Repo en GitHub
- New app → elegir repo → `app.py`
