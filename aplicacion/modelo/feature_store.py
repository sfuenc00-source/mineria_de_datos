# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Feature store hecho a mano: features versionadas en una tabla Delta (Práctica 3, Ej4)
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from modelo import pipeline_ml, tablas_delta

# Esquema de la tabla: Ticker, Date (clave), las columnas de features y version.
# Las features se guardan como columnas normales (no como el vector de MLlib): el VectorAssembler del
# pipeline vuelve a montar el vector al entrenar.


def existe_version(spark: SparkSession, ruta, version):
    # True si la tabla ya tiene filas de esa versión
    if not os.path.isdir(ruta):
        return False  # la tabla de features todavía no existe
    tabla = tablas_delta.leer_delta(spark, ruta)
    return tabla.filter(F.col("version") == int(version)).limit(1).count() > 0


def guardar_version(spark: SparkSession, features: DataFrame, ruta, version):
    # Guarda las features con su columna version (append). Una versión ya guardada NO se reescribe: las
    # versiones son inmutables, y si las features cambian se guarda con un número nuevo. Así ejecutar
    # el ejercicio varias veces no duplica filas. Devuelve True si ha escrito, False si ya existía.
    if existe_version(spark, ruta, version):
        return False
    tablas_delta.anadir_delta(features.withColumn("version", F.lit(int(version))), ruta)
    return True


def recuperar_version(spark: SparkSession, ruta, version, ticker, columnas) -> DataFrame:
    # Lee una versión concreta (en lugar de recalcular las features) con el mismo orden y reparto
    # que usa el entrenamiento, para que el resultado sea idéntico al original.
    tabla = tablas_delta.leer_delta(spark, ruta)
    filas = tabla.filter((F.col("version") == int(version)) & (F.col("Ticker") == ticker))
    return pipeline_ml.preparar_datos(filas.select("Ticker", "Date", *columnas))
