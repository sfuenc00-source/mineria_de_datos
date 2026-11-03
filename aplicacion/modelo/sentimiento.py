# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Carga del CSV de sentimiento por sector (Ej2-a)
from modelo import config


def cargar_sentimiento(spark):
    # Ej2-a: lee el CSV tal cual; sin inferSchema, todas las columnas llegan como string
    return (spark.read
            .option("header", True)
            .option("sep", config.SEPARADOR_CSV)
            .csv(config.RUTA_CSV_SENTIMIENTO))
