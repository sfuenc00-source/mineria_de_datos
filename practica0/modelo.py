import os
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F

RUTA_CSV = os.path.join(os.path.dirname(__file__), "ibex35_close-2024.csv")

def cargar_datos(spark):
    #Ej 1-a: lectura del CSV 
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .csv(RUTA_CSV))

def convertir_tipos(df):
    #Ej 1-a: Fecha a tipo date y el resto de columnas a tipo float
    df = df.withColumn("Fecha", to_date(df["Fecha"], "dd/MM/yyyy"))
    for column in df.columns[1:]:
        df = df.withColumn(column, col(f"`{column}`").cast(FloatType()))
    return df

def quitar_sufijos(df):
    #Ej 1-b: quitar sufijo ".MC" de los nombres de las columnas
    for column in df.columns:
        df = df.withColumnRenamed(column, column.replace(".MC", ""))
    return df

def eliminar_duplicados(df):
    #Ej 2-a: eliminar filas duplicadas
    return df.dropDuplicates()

def calcular_fechas(df):
    #Ej 2-b: calcular fecha mínima, máxima y total de días con información disponible
    min_date = df.agg({"Fecha": "min"}).collect()[0][0]
    max_date = df.agg({"Fecha": "max"}).collect()[0][0]
    total_days = (max_date - min_date).days + 1
    return min_date, max_date, total_days

def renombrar_columna(df, old_name, new_name):
    #Ej 3: renombrar columna
    return df.withColumnRenamed(old_name, new_name)

def calcular_estadisticas(df):
    #Ej 3: calcular media, máximo y mínimo anual para cada empresa
    return df.agg(
        *(
            [F.mean(F.col(f"`{c}`")).alias(f"{c}_Media_anual")
             for c in df.columns[1:]]
            +
            [F.max(F.col(f"`{c}`")).alias(f"{c}_Max_anual")
             for c in df.columns[1:]]
            +
            [F.min(F.col(f"`{c}`")).alias(f"{c}_Min_anual")
             for c in df.columns[1:]]
        )
    )



