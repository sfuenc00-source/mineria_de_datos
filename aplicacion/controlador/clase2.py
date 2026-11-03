from pyspark.sql import functions as F

from spark_session import create_spark_session
import db_connection
from modelo import config, yahoo, tablas_delta
from vista import consola as vista


def _primera_linea(error):
    # Primera línea no vacía del mensaje de error (los de Spark son larguísimos)
    lineas = [l for l in str(error).splitlines() if l.strip()]
    return lineas[0] if lineas else str(error)


def ejecutar():
    # Crear SparkSession (con Delta)
    spark = create_spark_session(jar_path=db_connection.JAR_PATH)
    ruta_ibex = config.RUTA_BRONZE_IBEX
    ruta_sent = config.RUTA_BRONZE_SENTIMIENTO

    # Ej3
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej3")
    tablas_delta.convertir_a_delta(spark, ruta_ibex)
    tablas_delta.convertir_a_delta(spark, ruta_sent)
    for nombre, ruta in (("ibex", ruta_ibex), ("sentimiento", ruta_sent)):
        vista.mostrar_mensaje(f"Historial de bronze/{nombre}:")
        vista.mostrar_historial(tablas_delta.historial(spark, ruta))
        vista.mostrar_mensaje(f"Contenido de _delta_log en bronze/{nombre}: {tablas_delta.listar_delta_log(ruta)}")
    vista.mostrar_comentario(
        "Ha aparecido la carpeta _delta_log con ficheros JSON de commit (el primero es "
        "00000000000000000000.json) que registran la conversión. Los ficheros Parquet no se han "
        "reescrito ni movido: convertir a Delta es una operación de metadatos, solo se añade el log "
        "encima de los datos que ya existían.")

    # Ej4
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej4")
    version_inicial = tablas_delta.version_actual(spark, ruta_ibex)
    filas_inicial = tablas_delta.leer_delta(spark, ruta_ibex).count()
    vista.mostrar_mensaje(f"Versión actual de bronze/ibex: {version_inicial} ({filas_inicial} filas)")
    vista.mostrar_historial(tablas_delta.historial(spark, ruta_ibex))

    df_nuevos, fallidos = yahoo.descargar_ibex(
        spark, config.TICKERS_IBEX, config.FECHA_INICIO, config.FECHA_FIN_HOY)
    if fallidos:
        vista.mostrar_mensaje(f"Tickers sin datos o con error: {fallidos}")

    # Primera ejecución del MERGE: deben insertarse los días nuevos
    tablas_delta.merge_nuevos(spark, ruta_ibex, df_nuevos)
    filas_merge1 = tablas_delta.leer_delta(spark, ruta_ibex).count()
    vista.mostrar_mensaje(f"MERGE 1: {filas_inicial} -> {filas_merge1} filas ({filas_merge1 - filas_inicial} nuevas)")

    # Segunda ejecución del MERGE: no debe añadir nada
    tablas_delta.merge_nuevos(spark, ruta_ibex, df_nuevos)
    filas_merge2 = tablas_delta.leer_delta(spark, ruta_ibex).count()
    vista.mostrar_mensaje(f"MERGE 2: {filas_merge1} -> {filas_merge2} filas ({filas_merge2 - filas_merge1} nuevas)")
    vista.mostrar_comentario(
        "En la primera ejecución el MERGE inserta los días que no estaban (el número de filas sube). "
        "En la segunda no cambia nada porque todas las claves (Ticker, Date) ya existen: "
        "la carga es idempotente, a diferencia de un append, que habría duplicado los datos.")
    version_ej4 = tablas_delta.version_actual(spark, ruta_ibex)  # estado al que volveremos en el Ej6
    vista.mostrar_mensaje(f"Versión de la tabla al terminar el Ej4: {version_ej4}")

    # Ej5-a
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej5-a")
    df_con_columna = tablas_delta.leer_delta(spark, ruta_ibex).withColumn("Close_mas5", F.col("Close") + 5)
    try:
        tablas_delta.anadir_delta(df_con_columna, ruta_ibex)
        vista.mostrar_mensaje("Delta ha ACEPTADO la escritura (no era lo esperado)")
    except Exception as e:
        vista.mostrar_mensaje(f"Delta rechaza la escritura: {_primera_linea(e)}")
    vista.mostrar_comentario(
        "Delta rechaza el append por schema enforcement: el DataFrame tiene una columna (Close_mas5) "
        "que la tabla no tiene, y por defecto no deja escribir datos con un esquema distinto.")

    # Ej5-b
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej5-b")
    tablas_delta.anadir_delta(df_con_columna, ruta_ibex, merge_schema=True)
    df_tras_5b = tablas_delta.leer_delta(spark, ruta_ibex)
    vista.mostrar_esquema(df_tras_5b)
    nulos = df_tras_5b.filter(F.col("Close_mas5").isNull()).count()
    vista.mostrar_mensaje(f"Filas totales: {df_tras_5b.count()} (con Close_mas5 nulo: {nulos})")
    vista.mostrar_comentario(
        "Con mergeSchema ya no lo rechaza: Delta incorpora Close_mas5 al esquema de la tabla "
        "(evolución de esquema) y las filas antiguas quedan con valor nulo en esa columna. "
        "Ojo: como el DataFrame era la tabla entera, el append también ha duplicado todas las filas.")

    # Ej5-c
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej5-c")
    condicion_bbva = F.col("Ticker") == "BBVA.MC"
    antes = tablas_delta.leer_delta(spark, ruta_ibex).filter(condicion_bbva)
    vista.mostrar_mensaje(f"Filas de BBVA antes del DELETE: {antes.count()}")
    vista.mostrar_filas(antes, 5)
    tablas_delta.borrar_ticker(spark, ruta_ibex, "BBVA.MC")
    despues = tablas_delta.leer_delta(spark, ruta_ibex).filter(condicion_bbva)
    vista.mostrar_mensaje(f"Filas de BBVA después del DELETE: {despues.count()}")
    vista.mostrar_filas(despues, 5)

    # Ej5-d
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej5-d")
    vista.mostrar_historial(tablas_delta.historial(spark, ruta_ibex))
    df_ej4 = tablas_delta.leer_delta(spark, ruta_ibex, version=version_ej4)
    df_actual = tablas_delta.leer_delta(spark, ruta_ibex)
    vista.mostrar_mensaje(f"Versión {version_ej4} (antes del Ej5-a): {df_ej4.count()} filas, columnas: {df_ej4.columns}")
    vista.mostrar_mensaje(f"Versión actual: {df_actual.count()} filas, columnas: {df_actual.columns}")
    vista.mostrar_comentario(
        "No está todo igual. La versión anterior al Ej5-a conserva los datos originales y no tiene "
        "la columna Close_mas5; la actual tiene esa columna, filas duplicadas por el append del Ej5-b "
        "y ya no tiene las filas de BBVA. El time travel permite comprobarlo porque el log guarda "
        "todas las versiones.")

    # Ej6
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej6")
    tablas_delta.restaurar_version(spark, ruta_ibex, version_ej4)
    df_restaurado = tablas_delta.leer_delta(spark, ruta_ibex)
    vista.mostrar_esquema(df_restaurado)
    vista.mostrar_mensaje(f"Filas tras el RESTORE: {df_restaurado.count()} (en la versión {version_ej4} había {df_ej4.count()})")
    vista.mostrar_historial(tablas_delta.historial(spark, ruta_ibex))
    vista.mostrar_comentario(
        "versionAsOf solo lee una versión antigua y no toca la tabla. RESTORE sí deja la tabla como "
        f"estaba en la versión {version_ej4} (con su esquema, sin Close_mas5, y con BBVA), pero lo hace "
        "creando un commit nuevo: el historial se conserva entero, incluido el propio RESTORE.")

    spark.stop()
