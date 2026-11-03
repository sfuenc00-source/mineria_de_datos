# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Operaciones con tablas Delta Lake (Clase 2: Ej3 a Ej6)
import os

from delta.tables import DeltaTable

# Clave para identificar una fila de precios (la columna de fecha se llama Date)
CONDICION_CLAVE = "d.Ticker = n.Ticker AND d.Date = n.Date"


def convertir_a_delta(spark, ruta):
    # Ej3: convierte una carpeta Parquet en tabla Delta SIN reescribir datos.
    # Solo añade la carpeta _delta_log. La ruta va entre backticks.
    return DeltaTable.convertToDelta(spark, f"parquet.`{ruta}`")


def historial(spark, ruta, ultimas=None):
    # Ej3 / Ej5-d: DataFrame con una fila por versión (la más reciente primero)
    tabla = DeltaTable.forPath(spark, ruta)
    return tabla.history(ultimas) if ultimas else tabla.history()


def version_actual(spark, ruta):
    # Número entero de la versión actual (no cambia nada en la tabla)
    return historial(spark, ruta, 1).select("version").collect()[0][0]


def listar_delta_log(ruta):
    # Ej3: ficheros que hay dentro de _delta_log
    return sorted(os.listdir(os.path.join(ruta, "_delta_log")))


def leer_delta(spark, ruta, version=None):
    # Lee la tabla Delta; con version, hace time travel (versionAsOf)
    lector = spark.read.format("delta")
    if version is not None:
        lector = lector.option("versionAsOf", version)
    return lector.load(ruta)


def guardar_delta(df, ruta, modo="overwrite"):
    # Escribe un DataFrame como tabla Delta (silver, gold...). Con overwrite se permite cambiar el esquema
    # (overwriteSchema), así se puede repetir el ejercicio las veces que haga falta.
    escritor = df.write.format("delta").mode(modo)
    if modo == "overwrite":
        escritor = escritor.option("overwriteSchema", "true")
    escritor.save(ruta)


def merge_nuevos(spark, ruta, df_nuevos):
    # Ej4: MERGE por (Ticker, Date). Solo inserta las filas que no existen en destino,
    # así que ejecutarlo dos veces no duplica datos (carga idempotente).
    destino = DeltaTable.forPath(spark, ruta)
    (destino.alias("d")
        .merge(df_nuevos.alias("n"), CONDICION_CLAVE)
        .whenNotMatchedInsertAll()
        .execute())


def anadir_delta(df, ruta, merge_schema=False):
    # Ej5-a / Ej5-b: append a la tabla Delta; con merge_schema=True se permite evolución de esquema
    escritor = df.write.format("delta").mode("append")
    if merge_schema:
        escritor = escritor.option("mergeSchema", "true")
    escritor.save(ruta)


def borrar_ticker(spark, ruta, ticker):
    # Ej5-c: DELETE de las filas de un ticker
    DeltaTable.forPath(spark, ruta).delete(f"Ticker = '{ticker}'")


def restaurar_version(spark, ruta, version):
    # Ej6: deja la tabla como estaba en esa versión (crea un commit nuevo, no borra historial)
    DeltaTable.forPath(spark, ruta).restoreToVersion(version)
