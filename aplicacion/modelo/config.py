# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Constantes de configuración de la aplicación
import os

from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType
)

# Carpeta "aplicacion" (un nivel por encima de la carpeta modelo)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _ruta(*partes):
    # Barras normales (/) también en Windows: Spark y Delta las aceptan sin problemas
    return os.path.join(BASE_DIR, *partes).replace("\\", "/")

# Capa bronze del data lake (datos crudos)
RUTA_BRONZE_IBEX = _ruta("data", "lake", "bronze", "ibex")
RUTA_BRONZE_SENTIMIENTO = _ruta("data", "lake", "bronze", "sentimiento")

# Capa silver (datos limpios y tipados)
RUTA_SILVER_IBEX = _ruta("data", "lake", "silver", "ibex")
RUTA_SILVER_SENTIMIENTO = _ruta("data", "lake", "silver", "sentimiento")

# Dimensión de empresas (datos extra de Yahoo): cruda en bronze, corregida en silver
RUTA_BRONZE_EMPRESAS = _ruta("data", "lake", "bronze", "dim_empresa")
RUTA_SILVER_EMPRESAS = _ruta("data", "lake", "silver", "dim_empresa")

# Capa gold: modelo en estrella (una tabla Delta por carpeta)
RUTA_GOLD_HECHOS = _ruta("data", "lake", "gold", "fact_cotizacion")
RUTA_GOLD_EMPRESAS = _ruta("data", "lake", "gold", "dim_empresa")
RUTA_GOLD_FECHAS = _ruta("data", "lake", "gold", "dim_fecha")

# Exportación de la gold a Parquet para Power BI
RUTA_EXPORT_GOLD = _ruta("data", "export", "gold")

# CSV de sentimiento por sector (entregado con la práctica)
RUTA_CSV_SENTIMIENTO = _ruta("sentimiento_ibex.csv")
SEPARADOR_CSV = ";"  # el CSV viene separado por punto y coma (Fecha;Sector;Sentimiento)

# Últimos 2 años; el último día es el 1 de octubre de 2026.
# yfinance trata "end" como EXCLUSIVO, así que se pone el día siguiente.
FECHA_INICIO = "2024-10-01"
FECHA_FIN = "2026-10-02"

# Clase 2 (Ej4): descargar hasta hoy, 20 de octubre de 2026 (de nuevo el final es exclusivo)
FECHA_FIN_HOY = "2026-10-21"

# Tickers del IBEX-35 (los mismos de la práctica 0)
TICKERS_IBEX = [
    "IBE.MC", "REP.MC", "NTGY.MC", "ELE.MC", "ENG.MC", "RED.MC", "SAN.MC",
    "BBVA.MC", "CABK.MC", "BKT.MC", "SAB.MC", "UNI.MC", "MAP.MC", "ACS.MC",
    "ANA.MC", "ANE.MC", "ACX.MC", "MTS.MC", "SCYR.MC", "CLNX.MC", "TEF.MC",
    "AENA.MC", "FER.MC", "ITX.MC", "AMS.MC", "IAG.MC", "GRF.MC", "FDR.MC",
    "SLR.MC", "ROVI.MC", "LOG.MC", "IDR.MC", "MEL.MC", "PUIG.MC", "COL.MC",
    "MRL.MC",
]

# Ej1-a: esquema explícito. El tercer argumento de StructField es "nullable".
ESQUEMA_HISTORICO = StructType([
    StructField("Date", DateType(), False),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", IntegerType(), True),
    StructField("Ticker", StringType(), False),
    StructField("Dividends", DoubleType(), False),
    StructField("Stock_Splits", DoubleType(), False),
])

# Ej1-a: por qué cada columna admite (o no) nulos
COMENTARIOS_NULOS = {
    "Date": "NO admite nulos: es el día de la sesión, la clave temporal de cada fila.",
    "Open": "Admite nulos: Yahoo puede devolver NaN en sesiones con datos incompletos.",
    "High": "Admite nulos: igual que Open, un dato puede faltar en una sesión.",
    "Low": "Admite nulos: igual que Open, un dato puede faltar en una sesión.",
    "Close": "Admite nulos: igual que Open, un dato puede faltar en una sesión.",
    "Volume": "Admite nulos: hay días o activos en los que Yahoo no informa del volumen.",
    "Ticker": "NO admite nulos: identifica de qué activo es la fila; sin él no se sabe a qué empresa pertenece.",
    "Dividends": "NO admite nulos: se rellena con 0.0 cuando no hay dividendo ese día.",
    "Stock_Splits": "NO admite nulos: se rellena con 0.0 cuando no hay desdoblamiento ese día.",
}

# Ej8: traducción de sectores en español al nombre que usa Yahoo (claves en minúsculas y sin espacios sobrantes).
# Los nombres que ya están en inglés no se tocan, solo se normalizan.
SECTORES_ES_EN = {
    "materiales básicos": "Basic Materials", "materiales basicos": "Basic Materials",
    "servicios de comunicación": "Communication Services", "servicios de comunicacion": "Communication Services",
    "comunicaciones": "Communication Services",
    "consumo cíclico": "Consumer Cyclical", "consumo ciclico": "Consumer Cyclical",
    "consumo defensivo": "Consumer Defensive",
    "criptomonedas": "Crypto", "cripto": "Crypto",
    "energía": "Energy", "energia": "Energy",
    "servicios financieros": "Financial Services", "finanzas": "Financial Services",
    "salud": "Healthcare", "sanidad": "Healthcare",
    "industriales": "Industrials", "industria": "Industrials",
    "inmobiliario": "Real Estate", "bienes raíces": "Real Estate", "bienes raices": "Real Estate",
    "tecnología": "Technology", "tecnologia": "Technology",
    "servicios públicos": "Utilities", "servicios publicos": "Utilities",
}

# Ej7/Ej9: datos "del experto" que se guardan de Ticker.info (campos tal cual los da Yahoo)
ESQUEMA_EMPRESAS_BRONZE = StructType([
    StructField("Ticker", StringType(), False),
    StructField("longName", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("country", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("marketCap", LongType(), True),
    StructField("beta", DoubleType(), True),
    StructField("trailingPE", DoubleType(), True),
    StructField("dividendYield", DoubleType(), True),
])
