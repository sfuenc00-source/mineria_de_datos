from spark_session import create_spark_session
import db_connection
from modelo import config, yahoo, bronze, sentimiento
from vista import consola as vista


def ejecutar(parte="todo"):
    # parte: "todo" (por defecto), "ej1" o "ej2" -> permite repetir solo el Ej2 sin volver a descargar
    spark = create_spark_session(jar_path=db_connection.JAR_PATH)
    if parte in ("todo", "ej1"):
        _ejercicio1(spark)
    if parte in ("todo", "ej2"):
        _ejercicio2(spark)

def _ejercicio1(spark):
    # Ej1-a
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej1-a")
    df_bbva = yahoo.descargar_historico(spark, "BBVA.MC", config.FECHA_INICIO, config.FECHA_FIN)
    vista.mostrar_esquema(df_bbva)
    vista.mostrar_filas(df_bbva, 5)
    vista.mostrar_nulos(config.COMENTARIOS_NULOS)

    # Ej1-b
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej1-b")
    df_ibex, fallidos = yahoo.descargar_ibex(
        spark, config.TICKERS_IBEX, config.FECHA_INICIO, config.FECHA_FIN)
    bronze.guardar_parquet(df_ibex, config.RUTA_BRONZE_IBEX)
    vista.mostrar_mensaje(f"Tickers guardados en bronze: {len(config.TICKERS_IBEX) - len(fallidos)}")
    if fallidos:
        vista.mostrar_mensaje(f"Tickers sin datos o con error: {fallidos}")

    # Ej1-c (opcional)
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej1-c")
    df_leido = bronze.leer_parquet(spark, config.RUTA_BRONZE_IBEX)
    vista.mostrar_esquema(df_leido)
    vista.mostrar_filas(df_leido, 5)
    df_columnas = bronze.leer_parquet(spark, config.RUTA_BRONZE_IBEX, ["Ticker", "Date", "Close"])
    vista.mostrar_filas(df_columnas, 5)
    vista.mostrar_comentario(
        "Parquet guarda los datos por columnas, así que al pedir solo Ticker, Date y Close "
        "Spark lee únicamente esos 3 column chunks y se salta los otros 6 (column pruning). "
        "Se lee menos disco y se usa menos memoria, y la lectura es más rápida. "
        "Con un formato por filas (como CSV) habría que leer todas las columnas igualmente.")



def _ejercicio2(spark):
    # Ej2-a
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej2-a")
    df_sentimiento = sentimiento.cargar_sentimiento(spark)
    vista.mostrar_esquema(df_sentimiento)
    vista.mostrar_filas(df_sentimiento, 5)
    vista.mostrar_comentario(
        "Todas las columnas llegan como string porque un CSV es texto plano y no guarda los tipos. "
        "Spark no los adivina salvo que se use inferSchema o se defina un esquema, "
        "así que fechas y números habrá que convertirlos más adelante en la limpieza (capa silver).")

    # Ej2-b
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej2-b")
    bronze.guardar_parquet(df_sentimiento, config.RUTA_BRONZE_SENTIMIENTO)
    filas = bronze.contar_filas(spark, config.RUTA_BRONZE_SENTIMIENTO)
    vista.mostrar_mensaje(f"Filas escritas en bronze/sentimiento: {filas}")

    # Ej2-c (opcional)
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej2-c")
    antes = bronze.contar_filas(spark, config.RUTA_BRONZE_SENTIMIENTO)
    bronze.guardar_parquet(df_sentimiento, config.RUTA_BRONZE_SENTIMIENTO, modo="append")
    despues = bronze.contar_filas(spark, config.RUTA_BRONZE_SENTIMIENTO)
    vista.mostrar_mensaje(f"Filas antes del append: {antes}")
    vista.mostrar_mensaje(f"Filas después del append: {despues}")
    vista.mostrar_comentario(
        "Las filas se duplican. El modo append no modifica nada, solo escribe ficheros Parquet nuevos "
        "en la carpeta, y Parquet por sí solo no comprueba duplicados ni sabe que esos datos ya estaban. "
        "Por eso ejecutar la carga varias veces en append ensucia la tabla.")
    # Dejar bronze/sentimiento limpio para las siguientes clases
    bronze.guardar_parquet(df_sentimiento, config.RUTA_BRONZE_SENTIMIENTO)
    vista.mostrar_mensaje("bronze/sentimiento restaurado con overwrite (sin duplicados).")
