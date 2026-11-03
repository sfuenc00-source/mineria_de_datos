# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Lectura y escritura de la capa bronze en Parquet (Ej1-b, Ej1-c, Ej2-b, Ej2-c)


def guardar_parquet(df, ruta, modo="overwrite", particiones=1):
    # Escribe un DataFrame en Parquet.
    # Son pocos datos, así que se junta todo en 1 partición (1 fichero) en vez de 16 pequeños.
    df.coalesce(particiones).write.mode(modo).parquet(ruta)


def leer_parquet(spark, ruta, columnas=None):
    # Ej1-c: lee un Parquet; si se indican columnas, solo lee esas (column pruning)
    df = spark.read.parquet(ruta)
    if columnas:
        df = df.select(*columnas)
    return df


def contar_filas(spark, ruta):
    # Ej2-b / Ej2-c: número de filas de una carpeta Parquet
    return spark.read.parquet(ruta).count()
