import os
import sys

from pyspark.sql import functions as F

from spark_session import create_spark_session
import db_connection
from modelo import config, yahoo, silver, gold, tablas_delta
from vista import consola as vista

# Datos de Ticker.info que pueden interesar además de los que guardamos (Ej7)
CAMPOS_INTERESANTES = [
    "sector", "industry", "country", "marketCap", "beta", "trailingPE", "forwardPE", "dividendYield",
    "priceToBook", "returnOnEquity", "debtToEquity", "fullTimeEmployees",
    "averageAnalystRating", "recommendationKey", "targetMeanPrice", "numberOfAnalystOpinions",
]

# Los datos de Yahoo se descargan una sola vez por ejecución (Ej7) y se reutilizan en el Ej9
_cache = {}


def _info_empresas(spark):
    if "info" not in _cache:
        _cache["info"] = yahoo.descargar_info_empresas(spark, config.TICKERS_IBEX)
    return _cache["info"]


def _leer(spark, ruta):
    return tablas_delta.leer_delta(spark, ruta)


def _ej7(spark):
    # Ej7
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej7")
    df_info, campos = _info_empresas(spark)
    vista.mostrar_mensaje(f"Yahoo ofrece {len(campos)} campos por empresa en Ticker.info")
    vista.mostrar_mensaje(f"Campos interesantes disponibles: {[c for c in CAMPOS_INTERESANTES if c in campos]}")
    vista.mostrar_filas(df_info, 5)

    precios = _leer(spark, config.RUTA_BRONZE_IBEX)
    resumen = precios.agg(F.countDistinct("Ticker").alias("t"), F.min("Date").alias("a"), F.max("Date").alias("b")).first()
    vista.mostrar_mensaje(f"bronze/ibex: {precios.count()} filas, {resumen['t']} tickers, del {resumen['a']} al {resumen['b']}")
    sentimiento = _leer(spark, config.RUTA_BRONZE_SENTIMIENTO)
    resumen = sentimiento.agg(F.countDistinct("Sector").alias("s"), F.min("Fecha").alias("a"), F.max("Fecha").alias("b")).first()
    vista.mostrar_mensaje(f"bronze/sentimiento: {sentimiento.count()} filas, {resumen['s']} nombres de sector distintos, del {resumen['a']} al {resumen['b']}")
    vista.mostrar_comentario(
        "El histórico de precios no dice a qué sector pertenece cada empresa, y el sentimiento viene por sector: "
        "sin el dato del experto (Ticker.info -> sector) no se pueden cruzar. Por eso guardo una dimensión de empresa "
        "con el sector, la industria, el país, la divisa, la capitalización, la beta y el PER. Además de cruzar, "
        "sirven para filtrar y agrupar en el dashboard (por sector, por país) y para ponderar por tamaño. "
        "Otros datos útiles que ofrece la API son las opiniones de analistas (averageAnalystRating, targetMeanPrice) "
        "o ratios como priceToBook o debtToEquity. Hay que tener cuidado con la calidad del dato: Yahoo clasifica "
        "algunas empresas de forma discutible (p. ej. Cellnex como Real Estate), hay empresas del IBEX que no son "
        "españolas (país), el PER viene vacío en algunas, y dividendYield viene en porcentaje en esta versión de "
        "yfinance (3.06 significa 3,06 %).",
        "Documentación de yfinance (Ticker.info)")


def _ej8(spark):
    # Ej8
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej8")
    bronce = _leer(spark, config.RUTA_BRONZE_SENTIMIENTO)
    limpio = silver.limpiar_sentimiento(bronce)
    tablas_delta.guardar_delta(limpio, config.RUTA_SILVER_SENTIMIENTO)
    plata = _leer(spark, config.RUTA_SILVER_SENTIMIENTO)
    vista.mostrar_mensaje(f"Filas bronze/sentimiento: {bronce.count()}")
    vista.mostrar_mensaje(f"Filas silver/sentimiento: {plata.count()}")
    vista.mostrar_mensaje(f"Nombres de sector distintos: {bronce.select('Sector').distinct().count()} en bronze -> "
                          f"{plata.select('Sector').distinct().count()} en silver")
    vista.mostrar_esquema(plata)
    vista.mostrar_filas(plata.orderBy("Fecha", "Sector"), 5)
    vista.mostrar_comentario(
        "Bronze guarda el CSV tal cual (todo texto). En silver: 1) Fecha pasa a date y Sentimiento a double; "
        "2) se unifica la coma y el punto decimal con regexp_replace; 3) el sector se normaliza (se quitan espacios "
        "sobrantes, se unifican mayúsculas y minúsculas con initcap y, si estuviera en español, se traduce al nombre "
        "que usa Yahoo), de modo que los 48 nombres distintos quedan en los 12 sectores reales; 4) se eliminan nulos "
        "(dropna) y duplicados (dropDuplicates); 5) como al unificar sectores pueden coincidir dos valores para el "
        "mismo día y sector, se promedian para dejar una fila por (Fecha, Sector), que es la clave del cruce con los "
        "precios.",
        "Práctica 0 y documentación de PySpark (regexp_replace, initcap, dropna, dropDuplicates, unpivot)")


def _ej9(spark):
    # Ej9
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej9")
    bronce_precios = _leer(spark, config.RUTA_BRONZE_IBEX)
    tablas_delta.guardar_delta(silver.limpiar_precios(bronce_precios), config.RUTA_SILVER_IBEX)

    # Dimensión de empresas: cruda en bronze y corregida en silver
    df_info, _ = _info_empresas(spark)
    tablas_delta.guardar_delta(df_info, config.RUTA_BRONZE_EMPRESAS)
    bronce_empresas = _leer(spark, config.RUTA_BRONZE_EMPRESAS)
    tablas_delta.guardar_delta(silver.limpiar_empresas(bronce_empresas), config.RUTA_SILVER_EMPRESAS)

    plata_precios = _leer(spark, config.RUTA_SILVER_IBEX)
    plata_empresas = _leer(spark, config.RUTA_SILVER_EMPRESAS)
    vista.mostrar_mensaje(f"Filas bronze/ibex: {bronce_precios.count()} -> silver/ibex: {plata_precios.count()}")
    vista.mostrar_mensaje(f"Filas bronze/dim_empresa: {bronce_empresas.count()} -> silver/dim_empresa: {plata_empresas.count()}")
    vista.mostrar_mensaje("Filas de silver/ibex:")
    vista.mostrar_filas(plata_precios.orderBy("Ticker", "Date"), 5)
    vista.mostrar_mensaje("Filas de silver/dim_empresa:")
    vista.mostrar_filas(plata_empresas.orderBy("Ticker"), 5)
    vista.mostrar_comentario(
        "Los precios de Yahoo ya vienen tipados y limpios, así que en silver solo se validan: sin nulos en Ticker, "
        "Date ni Close, cierre mayor que 0 y una única fila por (Ticker, Date). Los datos extra de Yahoo (Ej7) van "
        "en una dimensión aparte para no mezclar atributos de la empresa con las cotizaciones diarias: en bronze "
        "se guardan tal cual los da Yahoo y en silver se renombran las columnas, se normaliza el sector con la "
        "misma función que el sentimiento y se rellenan los textos vacíos (Sin sector, Desconocido).")


def _ej10(spark):
    # Ej10
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej10")
    precios = _leer(spark, config.RUTA_SILVER_IBEX)
    empresas = _leer(spark, config.RUTA_SILVER_EMPRESAS)
    sentimiento = _leer(spark, config.RUTA_SILVER_SENTIMIENTO)

    hechos = gold.construir_hechos(precios, empresas, sentimiento)
    tablas_delta.guardar_delta(hechos, config.RUTA_GOLD_HECHOS)
    hechos = _leer(spark, config.RUTA_GOLD_HECHOS)
    tablas_delta.guardar_delta(empresas, config.RUTA_GOLD_EMPRESAS)
    tablas_delta.guardar_delta(gold.construir_dim_fecha(hechos), config.RUTA_GOLD_FECHAS)

    vista.mostrar_mensaje(f"Filas silver/ibex: {precios.count()} -> gold/fact_cotizacion: {hechos.count()} "
                          "(deben coincidir: el cruce no pierde ni duplica cotizaciones)")
    vista.mostrar_mensaje(f"Cotizaciones sin sentimiento: {hechos.filter(F.col('Sentimiento').isNull()).count()}")
    vista.mostrar_mensaje(f"Filas gold/dim_empresa: {_leer(spark, config.RUTA_GOLD_EMPRESAS).count()}")
    vista.mostrar_mensaje(f"Filas gold/dim_fecha: {_leer(spark, config.RUTA_GOLD_FECHAS).count()}")
    vista.mostrar_filas(hechos.orderBy("Ticker", "Date"), 5)
    vista.mostrar_comentario(
        "La gold es un modelo en estrella con tres tablas Delta: fact_cotizacion (una fila por empresa y día, con "
        "las medidas: precios, volumen, dividendos y sentimiento), dim_empresa (clave Ticker: nombre, sector, "
        "industria, país...) y dim_fecha (clave Date: año, trimestre, mes, día de la semana...). El sentimiento se "
        "cruza por (Date, Sector) usando el sector de la dimensión de empresa, con un left join para conservar todas "
        "las cotizaciones aunque ese día no haya sentimiento. El hecho solo guarda claves y números; el texto "
        "descriptivo vive en las dimensiones.")


def _ej11(spark):
    # Ej11 (opcional)
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej11")
    precios = gold.precios_con_sector(_leer(spark, config.RUTA_SILVER_IBEX), _leer(spark, config.RUTA_SILVER_EMPRESAS))
    sentimiento = _leer(spark, config.RUTA_SILVER_SENTIMIENTO)

    sin_sentimiento = gold.precios_sin_sentimiento(precios, sentimiento)
    r = sin_sentimiento.agg(F.min("Date").alias("a"), F.max("Date").alias("b")).first()
    vista.mostrar_mensaje(f"(a) Precios sin sentimiento: {sin_sentimiento.count()} filas, del {r['a']} al {r['b']}")
    vista.mostrar_filas(sin_sentimiento.groupBy("Sector").count().orderBy(F.desc("count")), 15)

    sin_precios = gold.sentimiento_sin_precios(precios, sentimiento)
    r = sin_precios.agg(F.min("Date").alias("a"), F.max("Date").alias("b")).first()
    vista.mostrar_mensaje(f"(b) Sentimiento sin precios: {sin_precios.count()} filas, del {r['a']} al {r['b']}")
    vista.mostrar_filas(sin_precios.groupBy("Sector").count().orderBy(F.desc("count")), 15)
    vista.mostrar_filas(sin_precios.orderBy(F.desc("Date")), 5)
    vista.mostrar_comentario(
        "(a) Son cotizaciones de días en los que el CSV todavía no tiene sentimiento para el sector de la empresa "
        "(el CSV termina antes que el histórico de precios). Con un left join se conservan en la gold con el "
        "sentimiento a nulo; no relleno el hueco para no inventar datos, aunque se podría arrastrar el último "
        "valor conocido. (b) Es sentimiento de días o sectores en los que no hay ninguna cotización: fechas fuera "
        "del rango de precios descargado (anteriores a octubre de 2024), días sin sesión y sectores que no tienen "
        "empresas en el IBEX (p. ej. Crypto). Se descarta en la gold porque el hecho es la cotización, pero sigue "
        "disponible en silver.",
        "Documentación de PySpark (join con how='left_anti')")


def _ej12(spark):
    # Ej12 (opcional)
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej12")
    hechos = _leer(spark, config.RUTA_GOLD_HECHOS)
    tablas_delta.guardar_delta(gold.anadir_indicadores(hechos), config.RUTA_GOLD_HECHOS)
    con_indicadores = _leer(spark, config.RUTA_GOLD_HECHOS)
    vista.mostrar_esquema(con_indicadores)
    vista.mostrar_filas(
        con_indicadores.filter(F.col("Ticker") == "BBVA.MC").orderBy(F.desc("Date"))
        .select("Ticker", "Date", "Close", "Variacion_Diaria_Pct", "Media_Movil_20", "Rango_Diario_Pct"), 5)
    vista.mostrar_historial(tablas_delta.historial(spark, config.RUTA_GOLD_HECHOS))
    vista.mostrar_comentario(
        "Se añaden tres columnas a la tabla de hechos, calculadas por empresa y ordenadas por fecha con funciones "
        "de ventana: Variacion_Diaria_Pct (cambio del cierre respecto al día anterior, en %), Media_Movil_20 (media "
        "del cierre de las últimas 20 sesiones, solo cuando hay 20 completas) y Rango_Diario_Pct (diferencia entre "
        "máximo y mínimo del día sobre el mínimo, como medida de volatilidad intradía). Calcularlas aquí y no en "
        "Power BI evita repetir el cálculo en cada informe. Delta guarda la tabla anterior como una versión más.")


def _ej13(spark):
    # Ej13
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej13")
    tablas = [
        ("fact_cotizacion", config.RUTA_GOLD_HECHOS, ["Ticker", "Date"]),
        ("dim_empresa", config.RUTA_GOLD_EMPRESAS, ["Ticker"]),
        ("dim_fecha", config.RUTA_GOLD_FECHAS, ["Date"]),
    ]
    for nombre, ruta_delta, orden in tablas:
        destino = f"{config.RUTA_EXPORT_GOLD}/{nombre}"
        filas = gold.exportar_parquet(spark, ruta_delta, destino, orden)
        leidas = spark.read.parquet(destino).count()
        ficheros = gold.ficheros_parquet(destino)
        kb = sum(os.path.getsize(os.path.join(destino, f)) for f in ficheros) / 1024
        vista.mostrar_mensaje(f"{nombre}: {filas} filas exportadas, {leidas} leídas de vuelta, "
                              f"{len(ficheros)} fichero(s) Parquet, {kb:.0f} KB")
    vista.mostrar_comentario(
        "Se exporta cada tabla de la estrella como Parquet normal (sin _delta_log) en su propia carpeta de "
        "data/export/gold: Power BI lee Parquet pero no el log de Delta. Cada tabla va en un único fichero porque "
        "son pocos datos y muchos ficheros pequeños ralentizarían la lectura; no se particiona por la misma razón. "
        "Se mantiene el modelo en estrella (hecho con claves y números, dimensiones con el texto), ya que Power BI "
        "relaciona las tablas por Ticker y Date y comprime mucho mejor las columnas numéricas que las repeticiones "
        "de texto de una tabla plana; el hecho se ordena por Ticker y Date para comprimir mejor. Los tipos se "
        "conservan (fechas como date, números como double/int).",
        "Documentación de Power BI sobre el modelo en estrella")


EJERCICIOS = {
    "ej7": _ej7, "ej8": _ej8, "ej9": _ej9, "ej10": _ej10,
    "ej11": _ej11, "ej12": _ej12, "ej13": _ej13,
}


def ejecutar(*partes):
    # Sin argumentos ejecuta del Ej7 al Ej13 en orden; con argumentos, solo esos (p. ej. "ej10" "ej11")
    elegidos = [p.lower() for p in partes] or list(EJERCICIOS)
    desconocidos = [p for p in elegidos if p not in EJERCICIOS]
    if desconocidos:
        sys.exit(f"Ejercicio(s) desconocido(s): {desconocidos}. Opciones: {list(EJERCICIOS)}")

    spark = create_spark_session(jar_path=db_connection.JAR_PATH)
    for nombre in elegidos:
        EJERCICIOS[nombre](spark)
    spark.stop()
