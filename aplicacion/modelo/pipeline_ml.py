# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Pipeline de Spark MLlib con KMeans y sus métricas (Práctica 3, Ej1 y Ej2)
import random

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def fijar_semillas(semilla):
    # Fija el azar de Python y de NumPy. Spark no usa estos generadores: su azar se controla
    # con el parámetro seed de cada algoritmo (KMeans) y el de Hyperopt con rstate.
    random.seed(semilla)
    np.random.seed(semilla)


def preparar_datos(datos: DataFrame) -> DataFrame:
    # Deja los datos siempre igual de ordenados y repartidos. La inicialización de KMeans depende del orden y
    # de cómo estén repartidos los datos entre particiones (aunque la seed sea la misma). Son pocos datos, así
    # que se ordenan por fecha y se dejan en una sola partición: el resultado es el mismo en cualquier equipo,
    # con cualquier configuración de Spark y tanto con los datos de la gold como con los del feature store.
    return datos.orderBy("Date").coalesce(1)


def cargar_datos(hechos: DataFrame, ticker, columnas) -> DataFrame:
    # Datos de una empresa de la gold: clave (Ticker, Date) más las columnas elegidas, sin nulos
    return preparar_datos(hechos
                          .filter(F.col("Ticker") == ticker)
                          .select("Ticker", "Date", *columnas)
                          .dropna())


def construir_pipeline(columnas, k, semilla, escalar=False):
    # Pipeline: VectorAssembler (Transformer) -> [StandardScaler (Estimator)] -> KMeans (Estimator).
    # Open y Close están en la misma unidad (euros), por eso por defecto no se escala; con escalar=True
    # se añade el StandardScaler (útil si se mezclan columnas con escalas distintas).
    ensamblador = VectorAssembler(inputCols=columnas, outputCol="features")
    etapas = [ensamblador]
    columna_kmeans = "features"
    if escalar:
        etapas.append(StandardScaler(inputCol="features", outputCol="features_escaladas",
                                     withMean=True, withStd=True))
        columna_kmeans = "features_escaladas"
    etapas.append(KMeans(featuresCol=columna_kmeans, predictionCol="prediction", k=int(k), seed=semilla))
    return Pipeline(stages=etapas)


def entrenar(datos: DataFrame, columnas, k, semilla, escalar=False):
    # Un solo fit entrena todo el pipeline y devuelve un PipelineModel con todas las etapas
    pipeline = construir_pipeline(columnas, k, semilla, escalar)
    modelo = pipeline.fit(datos)
    predicciones = modelo.transform(datos)
    return modelo, predicciones


def columna_features(modelo):
    # Columna que usa el KMeans (la del scaler si lo hay, si no la del assembler)
    return modelo.stages[-1].getFeaturesCol()


def silhouette(modelo, predicciones):
    # Silhouette (de -1 a 1): cuanto más cerca de 1, más separados y compactos están los grupos
    evaluador = ClusteringEvaluator(featuresCol=columna_features(modelo),
                                    predictionCol="prediction", metricName="silhouette")
    return float(evaluador.evaluate(predicciones))


def davies_bouldin(modelo, predicciones):
    # Índice de Davies-Bouldin (Ej2): Spark solo trae la silhouette, así que se calcula a mano.
    # Para cada grupo i: s_i = distancia media de sus puntos a su centroide (dispersión).
    # Para cada par: (s_i + s_j) / distancia(c_i, c_j). Cada grupo se queda con su peor pareja
    # (la máxima) y el índice es la media de esos máximos. MENOR es mejor (0 sería ideal).
    kmeans = modelo.stages[-1]
    centros = [np.array(c) for c in kmeans.clusterCenters()]
    if len(centros) < 2:
        raise ValueError("Davies-Bouldin necesita al menos 2 clusters")

    @F.udf(DoubleType())
    def distancia_al_centro(features, cluster):
        return float(np.linalg.norm(features.toArray() - centros[cluster]))

    filas = (predicciones
             .withColumn("d", distancia_al_centro(columna_features(modelo), "prediction"))
             .groupBy("prediction").agg(F.avg("d").alias("s"))
             .collect())
    dispersion = {fila["prediction"]: fila["s"] for fila in filas}

    indices = []
    for i in dispersion:
        razones = [(dispersion[i] + dispersion[j]) / np.linalg.norm(centros[i] - centros[j])
                   for j in dispersion if j != i]
        if razones:
            indices.append(max(razones))
    return float(np.mean(indices))


def tamanos_clusters(predicciones):
    # Número de días que hay en cada cluster
    filas = predicciones.groupBy("prediction").count().orderBy("prediction").collect()
    return {fila["prediction"]: fila["count"] for fila in filas}


def centros(modelo):
    # Centroides del KMeans (en el espacio de la columna de entrada del KMeans)
    return [list(map(float, c)) for c in modelo.stages[-1].clusterCenters()]


def clasificar(modelo, nuevos: DataFrame, columnas):
    # Aplica un PipelineModel (recuperado de MLflow o recién entrenado) a datos nuevos:
    # el propio pipeline monta el vector de features y devuelve el cluster de cada fila
    return modelo.transform(nuevos).select(*columnas, "prediction")


def mismo_resultado(modelo_a, predicciones_a, modelo_b, predicciones_b):
    # Ej4: True si dos entrenamientos dan el mismo resultado: cada día cae en el mismo cluster y los
    # centroides coinciden
    asignacion_a = [(f["Date"], f["prediction"]) for f in predicciones_a.select("Date", "prediction").orderBy("Date").collect()]
    asignacion_b = [(f["Date"], f["prediction"]) for f in predicciones_b.select("Date", "prediction").orderBy("Date").collect()]
    mismos_centros = np.allclose(np.array(centros(modelo_a)), np.array(centros(modelo_b)))
    return asignacion_a == asignacion_b and bool(mismos_centros)
