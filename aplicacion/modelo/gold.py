# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Capa gold: modelo en estrella para Power BI (Clase 3: Ej10 a Ej13)
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
         "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def precios_con_sector(precios: DataFrame, empresas: DataFrame) -> DataFrame:
    # Añade el sector de cada empresa a sus precios (el sector no viene en el histórico de Yahoo)
    return precios.join(empresas.select("Ticker", "Sector"), "Ticker", "left")


def construir_hechos(precios: DataFrame, empresas: DataFrame, sentimiento: DataFrame) -> DataFrame:
    # Ej10: tabla de hechos = una fila por empresa y día, con el sentimiento de su sector ese día.
    # Es un left join: no se pierde ninguna cotización aunque ese día no haya sentimiento.
    con_sector = precios_con_sector(precios, empresas)
    sent = sentimiento.select(F.col("Fecha").alias("Date"), "Sector", "Sentimiento")
    cruce = con_sector.join(sent, ["Date", "Sector"], "left")
    # El sector se queda solo en la dimensión de empresa (el hecho guarda claves y medidas)
    return cruce.select("Ticker", "Date", "Open", "High", "Low", "Close", "Volume",
                        "Dividends", "Stock_Splits", "Sentimiento")


def construir_dim_fecha(hechos: DataFrame) -> DataFrame:
    # Ej10: dimensión de fecha con un día por fila (calendario completo entre la primera y la última cotización)
    rango = hechos.agg(F.min("Date").alias("inicio"), F.max("Date").alias("fin"))
    dias = rango.select(F.explode(F.sequence("inicio", "fin")).alias("Date"))
    dia_semana = ((F.dayofweek("Date") + 5) % 7) + 1  # lunes = 1 ... domingo = 7
    return dias.select(
        "Date",
        F.year("Date").alias("Anio"),
        F.quarter("Date").alias("Trimestre"),
        F.month("Date").alias("Mes"),
        F.element_at(F.array(*[F.lit(m) for m in MESES]), F.month("Date")).alias("NombreMes"),
        F.dayofmonth("Date").alias("Dia"),
        dia_semana.alias("DiaSemana"),
        F.element_at(F.array(*[F.lit(d) for d in DIAS]), dia_semana).alias("NombreDia"),
        (dia_semana >= 6).alias("EsFinDeSemana"),
    )


def precios_sin_sentimiento(precios_sector: DataFrame, sentimiento: DataFrame) -> DataFrame:
    # Ej11-a: precios (con su sector) cuyo día y sector no tienen sentimiento -> anti-join
    sent = sentimiento.select(F.col("Fecha").alias("Date"), "Sector")
    return precios_sector.join(sent, ["Date", "Sector"], "left_anti")


def sentimiento_sin_precios(precios_sector: DataFrame, sentimiento: DataFrame) -> DataFrame:
    # Ej11-b: sentimiento de un día y sector en los que no cotiza ninguna empresa -> anti-join
    claves = precios_sector.select("Date", "Sector").distinct()
    sent = sentimiento.select(F.col("Fecha").alias("Date"), "Sector", "Sentimiento")
    return sent.join(claves, ["Date", "Sector"], "left_anti")


def anadir_indicadores(hechos: DataFrame) -> DataFrame:
    # Ej12: tres columnas calculadas por empresa, ordenadas por fecha
    por_empresa = Window.partitionBy("Ticker").orderBy("Date")
    ultimos_20 = por_empresa.rowsBetween(-19, 0)
    cierre_anterior = F.lag("Close").over(por_empresa)
    variacion = (F.col("Close") - cierre_anterior) / cierre_anterior * 100
    # La media móvil solo se calcula cuando ya hay 20 sesiones completas
    media_20 = F.when(F.count("Close").over(ultimos_20) == 20, F.avg("Close").over(ultimos_20))
    rango = F.try_divide(F.col("High") - F.col("Low"), F.col("Low")) * 100
    return (hechos
            .withColumn("Variacion_Diaria_Pct", F.round(variacion, 4))
            .withColumn("Media_Movil_20", F.round(media_20, 4))
            .withColumn("Rango_Diario_Pct", F.round(rango, 4)))


def exportar_parquet(spark: SparkSession, ruta_delta: str, ruta_export: str, ordenar_por=None) -> int:
    # Ej13: lee una tabla Delta de la gold y la escribe como Parquet normal (sin _delta_log), en un único
    # fichero porque son pocos datos (muchos ficheros pequeños harían más lenta la lectura de Power BI).
    df = spark.read.format("delta").load(ruta_delta)
    if ordenar_por:
        df = df.orderBy(*ordenar_por)
    df.coalesce(1).write.mode("overwrite").parquet(ruta_export)
    return df.count()


def ficheros_parquet(ruta: str):
    # Nombres de los ficheros .parquet de una carpeta exportada
    return sorted(f for f in os.listdir(ruta) if f.endswith(".parquet"))
