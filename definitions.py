from dagster import asset,Definitions, load_assets_from_modules, load_asset_checks_from_modules
# Supongamos que tu archivo se llama proyecto_islas.py
import test_checks
import lab_renta


defs = Definitions(
    assets=load_assets_from_modules([lab_renta]),
    # ¡AQUÍ ESTÁ LA CLAVE! Debes añadir el check aquí:
    asset_checks=load_asset_checks_from_modules([test_checks]),
)