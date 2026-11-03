# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Descarga de datos de Yahoo Finance (Ej1-a y Ej1-b)
import math
import time
from functools import reduce

import pandas as pd
import yfinance as yf
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType

from modelo import config


def _convertir(valor, tipo):
    # Pasa un valor de pandas/numpy a un tipo Python que Spark acepte
    if valor is None or pd.isna(valor):
        return None
    if isinstance(tipo, DoubleType):
        return float(valor)
    if isinstance(tipo, IntegerType):
        return int(valor)
    return valor


def descargar_historico(spark, ticker, inicio, fin):
    # Ej1-a: descarga el histórico diario de un ticker y lo devuelve como DataFrame de Spark
    pdf = yf.Ticker(ticker).history(start=inicio, end=fin, interval="1d", auto_adjust=False)

    if pdf.empty:
        return spark.createDataFrame([], config.ESQUEMA_HISTORICO)

    pdf.reset_index(inplace=True)  # el índice Date pasa a ser una columna
    pdf["Date"] = pdf["Date"].dt.tz_localize(None).dt.date  # quitar zona horaria -> date
    pdf["Ticker"] = ticker
    # Delta no admite espacios en los nombres de columna: "Stock Splits" -> "Stock_Splits"
    pdf = pdf.rename(columns={"Stock Splits": "Stock_Splits"})
    pdf[["Dividends", "Stock_Splits"]] = pdf[["Dividends", "Stock_Splits"]].fillna(0.0)

    campos = config.ESQUEMA_HISTORICO.fields
    registros = [
        tuple(_convertir(valor, campo.dataType) for valor, campo in zip(fila, campos))
        for fila in pdf[[c.name for c in campos]].itertuples(index=False, name=None)
    ]
    return spark.createDataFrame(registros, config.ESQUEMA_HISTORICO)


def descargar_ibex(spark, tickers, inicio, fin):
    # Ej1-b: descarga todos los tickers y apila los DataFrames con union
    dataframes = []
    for ticker in tickers:
        try:
            dataframes.append(descargar_historico(spark, ticker, inicio, fin))
        except Exception:
            pass  # el ticker se detecta como fallido más abajo
    df_total = reduce(DataFrame.union, dataframes)

    # Un único trabajo de Spark para saber qué tickers sí tienen datos
    presentes = {fila["Ticker"] for fila in df_total.select("Ticker").distinct().collect()}
    fallidos = [t for t in tickers if t not in presentes]
    return df_total, fallidos


def _a_numero(valor):
    # float válido o None (Yahoo a veces devuelve NaN, infinito o texto)
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return None
    return None if (math.isnan(numero) or math.isinf(numero)) else numero


def _a_texto(valor):
    return None if valor is None else str(valor)


def descargar_info_empresas(spark, tickers):
    # Ej7: datos de la empresa que da Yahoo en Ticker.info. Devuelve (DataFrame crudo, campos disponibles)
    filas, campos = [], set()
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
        except Exception:
            info = {}
        campos |= set(info)
        capitalizacion = _a_numero(info.get("marketCap"))
        filas.append((
            ticker,
            _a_texto(info.get("longName")),
            _a_texto(info.get("sector")),
            _a_texto(info.get("industry")),
            _a_texto(info.get("country")),
            _a_texto(info.get("currency")),
            None if capitalizacion is None else int(capitalizacion),
            _a_numero(info.get("beta")),
            _a_numero(info.get("trailingPE")),
            _a_numero(info.get("dividendYield")),
        ))
        time.sleep(0.3)  # evita que Yahoo limite las peticiones
    return spark.createDataFrame(filas, config.ESQUEMA_EMPRESAS_BRONZE), sorted(campos)
