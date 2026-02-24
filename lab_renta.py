import pandas as pd
import os
from dagster import  asset, AssetExecutionContext
from plotnine import ggplot , aes, geom_col, theme_minimal, labs, theme, element_text


@asset

def leer_datos():
    return pd.read_csv("distribucion-renta-canarias.csv")

@asset
def limpiar_datos(leer_datos:pd.DataFrame):
    df = leer_datos.copy()

    df = df.rename(columns={
    "TIME_PERIOD#es": "anio",
    "TERRITORIO#es": "territorio",
    "MEDIDAS#es": "medida",
    "OBS_VALUE": "valor"
    })
    df["anio"] = df["anio"].astype(int)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    df = df.dropna(subset=["anio", "valor", "territorio", "medida"])

    
    df["territorio"] = df["territorio"].astype(str).str.strip().str.lower()
    df["medida"] = df["medida"].astype(str).str.strip().str.lower()

    return df

@asset
def renta_2022(limpiar_datos: pd.DataFrame):
    return limpiar_datos[limpiar_datos["anio"] == 2022].copy()

@asset 
def plot_renta_canarias_2022 (context: AssetExecutionContext, renta_2022: pd.DataFrame):
    p =(
        ggplot(renta_2022, aes(
        x="medida",
        y="valor"
        ))
        + geom_col()
        + theme_minimal()
        + labs(
            title="Distribución de renta en Canarias",
             subtitle="Año 2022",
             x="Tramo de renta",
            y="Valor",
            caption="Fuente: Instituto de Estadística de Canarias"
        )
    )
  
    os.makedirs("outputs", exist_ok=True)

    p.save("outputs/grafico_renta_Canarias_2022.png", dpi=150, width=12, height=6)
    print("Gráfico guardado en outputs/grafico_renta_Canarias_2022.png")




@asset
def leer_codislas():
    codislas =pd.read_csv("codislas.csv", encoding="cp1252", sep=";")
    if "ISLA" in codislas.columns:
        codislas["ISLA"] = codislas["ISLA"].astype(str).str.strip().str.lower()
    if "NOMBRE" in codislas.columns:
        codislas["NOMBRE"] = codislas["NOMBRE"].astype(str).str.strip()

    return codislas

@asset
def unir_datos(renta_2022: pd.DataFrame, leer_codislas: pd.DataFrame):
    df= renta_2022.merge(
        leer_codislas,
        left_on="territorio",
        right_on="ISLA",
        how="inner"
    )
    return df



@asset
def plot_renta_municipios_2022(context: AssetExecutionContext, unir_datos: pd.DataFrame):
    p = (
        ggplot(unir_datos, aes(x="medida", y="valor", fill="NOMBRE"))
        + geom_col(position="dodge")
        + theme_minimal()
        + theme(
            legend_position="bottom",
            legend_title=element_text(size=8),
            legend_text=element_text(size=6),
            legend_key_size=6,
        )
        + labs(
            title="Distribución de renta en Canarias por municipios",
            subtitle="Año 2022",
            x="Tramo de renta",
            y="Valor",
            fill="Municipio",
            caption="Fuente: Instituto de Estadística de Canarias",
        )
    )

    os.makedirs("outputs", exist_ok=True)

    p.save("outputs/grafico_renta_CanariasMunicipio.png", dpi=150, width=12, height=6)
    print("Gráfico guardado en outputs/grafico_renta_CanariasMunicipio.png")


@asset
def renta_pensiones_2022(unir_datos: pd.DataFrame):
    # Filtra solo la medida "Pensiones"
    df = unir_datos[unir_datos["medida"] == "pensiones"].copy()
    return df


@asset
def plot_pensiones_municipios_2022(context: AssetExecutionContext, renta_pensiones_2022: pd.DataFrame):

    p = (
        ggplot(renta_pensiones_2022, aes(x="medida", y="valor", fill="NOMBRE"))
        + geom_col(position="dodge")
        + theme_minimal()
        + theme(
            legend_position="bottom",
            legend_title=element_text(size=8),
            legend_text=element_text(size=6),
            legend_key_size=6,
        )
        + labs(
            title="Distribución de pensiones en Canarias por municipios",
            subtitle="Año 2022",
            x="Pensiones",
            y="Valor",
            fill="Municipio",
            caption="Fuente: Instituto de Estadística de Canarias",
        )
    )

    os.makedirs("outputs", exist_ok=True)

    p.save("outputs/grafico_renta_CanariasMunicipioPensiones.png", dpi=150, width=12, height=6)
    print("Gráfico guardado en outputs/grafico_renta_CanariasMunicipioPensiones.png")

