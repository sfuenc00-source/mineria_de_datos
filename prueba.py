from pyspark.sql import SparkSession

print("1. Creando sesión", flush=True)
spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("IBEX35")
    .getOrCreate()
)

print("2. Leyendo CSV", flush=True)
df = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .csv("C:/ruta/real/archivo.csv")
)

print("3. Mostrando filas", flush=True)
df.show(5, truncate=False)

print("4. Cerrando sesión", flush=True)
spark.stop()

print("5. Terminado", flush=True)