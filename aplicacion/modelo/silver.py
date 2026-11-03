# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Capa silver: limpiar, tipar y validar (Clase 3: Ej8 y Ej9)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from modelo import config


def normalizar_sector(columna):
    # Quita espacios sobrantes (también los de dentro), pasa a minúsculas, traduce si estaba en español
    # y deja el formato de Yahoo ("Basic Materials"). Así "  ENERGY", "energy" y "Energy" son el mismo sector.
    limpio = F.lower(F.trim(F.regexp_replace(columna, r"\s+", " ")))
    traduccion = None
    for espanol, ingles in config.SECTORES_ES_EN.items():
        condicion = limpio == espanol
        traduccion = F.when(condicion, ingles) if traduccion is None else traduccion.when(condicion, ingles)
    return traduccion.otherwise(F.initcap(limpio))


def a_formato_largo(df: DataFrame) -> DataFrame:
    # Si el sentimiento llegara en formato ancho (una columna por sector), lo pasa a largo con unpivot.
    # Si ya es largo (Fecha, Sector, Sentimiento), lo deja como está.
    if {"Fecha", "Sector", "Sentimiento"} <= set(df.columns):
        return df
    return df.unpivot("Fecha", None, "Sector", "Sentimiento")


def limpiar_sentimiento(df: DataFrame) -> DataFrame:
    # Ej8: tipa, unifica decimales (coma -> punto), normaliza el sector, quita nulos y duplicados.
    df = a_formato_largo(df)
    sentimiento = F.regexp_replace(F.trim(F.col("Sentimiento")), ",", ".")
    # try_cast: un valor imposible de convertir queda en nulo (y se elimina) en vez de romper la ejecución
    tipado = df.select(
        F.trim(F.col("Fecha")).try_cast("date").alias("Fecha"),
        normalizar_sector(F.col("Sector")).alias("Sector"),
        sentimiento.try_cast("double").alias("Sentimiento"),
    )
    sin_nulos = tipado.dropna()
    sin_duplicados = sin_nulos.dropDuplicates()
    # Tras unificar los sectores pueden quedar dos valores distintos para el mismo (Fecha, Sector):
    # se promedian para tener una única fila por día y sector (clave del cruce con los precios).
    return (sin_duplicados.groupBy("Fecha", "Sector")
            .agg(F.round(F.avg("Sentimiento"), 2).alias("Sentimiento")))


def limpiar_precios(df: DataFrame) -> DataFrame:
    # Ej9: los precios de Yahoo vienen limpios; se validan las claves y el cierre y se quitan duplicados.
    validos = df.dropna(subset=["Date", "Ticker", "Close"]).filter(F.col("Close") > 0)
    return validos.dropDuplicates(["Ticker", "Date"])


def limpiar_empresas(df: DataFrame) -> DataFrame:
    # Ej9: dimensión de empresas corregida (nombres de columna claros, sector normalizado, sin nulos en texto).
    return (df.select(
        F.trim(F.col("Ticker")).alias("Ticker"),
        F.col("longName").alias("Nombre"),
        F.coalesce(normalizar_sector(F.col("sector")), F.lit("Sin sector")).alias("Sector"),
        F.coalesce(F.trim(F.col("industry")), F.lit("Sin industria")).alias("Industria"),
        F.coalesce(F.trim(F.col("country")), F.lit("Desconocido")).alias("Pais"),
        F.col("currency").alias("Divisa"),
        F.col("marketCap").alias("CapitalizacionBursatil"),
        F.col("beta").alias("Beta"),
        F.col("trailingPE").alias("PER"),
        F.col("dividendYield").alias("RentabilidadDividendo"),
    ).dropDuplicates(["Ticker"]))
