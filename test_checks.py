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


@asset_check(asset="limpiar_datos")
def check_normalizacion_datos(limpiar_datos: pd.DataFrame):
    datos_territorio = (limpiar_datos["territorio"] == limpiar_datos["territorio"].str.strip().str.lower()).all()
    datos_medida = (limpiar_datos["medida"] == limpiar_datos["medida"].str.strip().str.lower()).all()

    passed = bool(datos_territorio and datos_medida)

    return AssetCheckResult(
        passed=passed,
        metadata={
            "territorio_normalizado": MetadataValue.text(str(datos_territorio)),
            "medida_normalizada": MetadataValue.text(str(datos_medida)),
            "explicacion": MetadataValue.text(
                "Normalizamos categorías y evitamos duplicados"
            ),
        },
    )

@asset_check(asset="renta_2022")
def check_anio_2022(renta_2022: pd.DataFrame):
    anios = sorted(renta_2022["anio"].unique().tolist()) if "anio" in renta_2022.columns else []
    passed = (anios == [2022])

    return AssetCheckResult(
        passed=passed,
        metadata={
            "anios_detectados": MetadataValue.text(str(anios)),
            "explicacion": MetadataValue.text("Filtramos por el año 2022."),
        },
    )
