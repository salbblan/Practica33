import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, MetadataValue, AssetExecutionContext
#import lab_renta



@asset_check(asset="leer_datos")

def check_no_vacio(leer_datos: pd.DataFrame):
    n = int(len(leer_datos))
    return AssetCheckResult(
        passed=(n > 0),
        metadata={
            "n_filas": MetadataValue.int(n),
            "explicacion": MetadataValue.text("El dataset no puede estar vacío."),
        },
    )
