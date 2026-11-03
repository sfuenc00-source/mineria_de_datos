import os
import sys

# Forzar que driver y workers usen el Python del entorno activo
# (evita el atajo de la Microsoft Store). Debe ir ANTES de crear la sesión.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session(app_name="IBEX35", jar_path=None):
    """
    Crea y devuelve una SparkSession con Delta Lake.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")  # por defecto 1g, se quedaba corto al escribir Parquet
        # Todo en local: evita que Spark use el nombre/IP de red del PC (VPN, wifi, varias tarjetas),
        # que es lo que suele causar el cuelgue del arranque (errores Heartbeater / BlockManagerId)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    if jar_path:
        builder = builder.config("spark.driver.extraClassPath", jar_path)
    return configure_spark_with_delta_pip(builder).getOrCreate()
