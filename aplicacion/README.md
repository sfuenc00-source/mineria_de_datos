# Aplicación IBEX-35 (Minería de Datos)

Proyecto de la asignatura Minería de Datos (GIDIA, Universidad de León) sobre cotizaciones del IBEX-35.
Autora: Sofía Fuentes Crespo · Noviembre de 2026

El proyecto tiene dos partes: la aplicación en PySpark que construye el lago de datos (Práctica 1)
y el análisis y visualización de esos datos en Power BI (Práctica 2).

## Estructura

| Ruta | Contenido |
|---|---|
| `main.py` | Punto de entrada de la aplicación. |
| `modelo/` | Acceso a datos y capas del lago: `yahoo.py`, `bronze.py`, `silver.py`, `gold.py`, `sentimiento.py`, `tablas_delta.py`, `config.py`. |
| `controlador/` | Lógica de cada clase de la Práctica 1: `clase1.py`, `clase2.py`, `clase3.py`. |
| `vista/` | Salida por consola (`consola.py`). |
| `spark_session.py`, `db_connection.py` | Sesión de Spark y conexión. |
| `data/lake/` | Lago de datos en Delta: capas `bronze`, `silver` y `gold`. |
| `data/export/gold/` | Exportación en Parquet de la capa gold (`fact_cotizacion`, `dim_empresa`, `dim_fecha`). |
| `sentimiento_ibex.csv` | Datos de sentimiento. |
| `salida_*.txt` | Salidas de ejecución de la Práctica 1. |
| `Memoria_Practica1.pdf` | Memoria de la Práctica 1. |

## Práctica 1: aplicación PySpark

Arquitectura MVC (modelo, controlador, vista). Los datos pasan por las capas bronze, silver y gold
y la capa gold queda como modelo en estrella.

## Práctica 2: visualización con Power BI

| Archivo | Descripción |
|---|---|
| `practica2_ibex.pbix` | Informe de Power BI con modelo en estrella (`fact_cotizacion`, `dim_empresa`, `dim_fecha`, relaciones 1:* en una dirección) y dashboard de 4 páginas: Panorama, Evolución, Sentimiento y calendario, Riesgo y retorno. |
| `Memoria_Practica2.pdf` | Memoria de la Práctica 2: diseño, análisis global y por sector, conclusiones y declaración de uso de IA. |

El informe lee los Parquet de `data/export/gold`. Si se abre en otro equipo, hay que actualizar la ruta de origen en
Power Query (Transformar datos → Configuración del origen de datos).
