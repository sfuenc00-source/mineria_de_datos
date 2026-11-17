# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Utilidades de MLflow: configuración, registro y consulta de runs (Práctica 3, Ej1 a Ej3)
import os
from pathlib import Path

# Las versiones recientes de MLflow (3.x) avisan de que el almacenamiento en carpeta (mlruns) está en
# "modo mantenimiento" y lanzan un error salvo que se permita de forma explícita. El enunciado pide guardar
# las runs en local con file:///, así que se activa aquí, ANTES de importar mlflow.
os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")

import mlflow  # noqa: E402
import mlflow.spark  # noqa: E402
import pyspark  # noqa: E402

from modelo import config

NOMBRE_MODELO = "modelo"  # nombre del artefacto del modelo dentro de cada run


def configurar(nombre_experimento):
    # Apunta MLflow a la carpeta local mlruns y crea (o selecciona) el experimento.
    # as_uri() genera bien la URI también en Windows (file:///C:/...).
    mlflow.set_tracking_uri(Path(config.RUTA_MLRUNS).as_uri())
    return mlflow.set_experiment(nombre_experimento).experiment_id


def registrar_intento(parametros, metricas, modelo):
    # Dentro de una run abierta (with mlflow.start_run()): guarda parámetros, métricas y el modelo.
    # El modelo queda como artefacto de la run, así se puede recuperar luego con cargar_modelo.
    mlflow.log_params(parametros)
    mlflow.log_metrics(metricas)
    # pip_requirements: si no se indica, MLflow intenta deducir las dependencias de cada modelo, tarda unos
    # 20 segundos por run y en Windows acaba fallando (usa pyspark igualmente). Indicarlas lo evita.
    mlflow.spark.log_model(modelo, NOMBRE_MODELO, pip_requirements=[f"pyspark=={pyspark.__version__}"])


def cargar_modelo(run_id):
    # Recupera de MLflow el PipelineModel guardado en una run
    return mlflow.spark.load_model(f"runs:/{run_id}/{NOMBRE_MODELO}")


def recuperar_runs(nombres_experimentos, metrica, ascendente=True):
    # Ej3: todas las runs de los experimentos indicados, ordenadas por el valor de una métrica.
    # Devuelve un DataFrame de pandas (columnas run_id, params.*, metrics.*, status...).
    # Las runs que no tienen esa métrica (valor nulo) quedan al final.
    mlflow.set_tracking_uri(Path(config.RUTA_MLRUNS).as_uri())
    sentido = "ASC" if ascendente else "DESC"
    return mlflow.search_runs(experiment_names=list(nombres_experimentos),
                              order_by=[f"metrics.{metrica} {sentido}"])


def listar_experimentos():
    # Ej5: nombres de los experimentos que hay en mlruns (sin el "Default" que crea MLflow vacío)
    mlflow.set_tracking_uri(Path(config.RUTA_MLRUNS).as_uri())
    return [e.name for e in mlflow.search_experiments() if e.name != "Default"]


def mejor_run_de(nombre_experimento):
    # Ej5: la mejor run de un experimento y la métrica con la que se ha elegido. La principal es
    # davies_bouldin (menor es mejor); si el experimento no la tiene (el del Ej1 solo guarda la silhouette),
    # se usa la silhouette (mayor es mejor). Devuelve (run, métrica), o (None, None) si no hay runs con métrica.
    for metrica, ascendente in (("davies_bouldin", True), ("silhouette", False)):
        runs = recuperar_runs([nombre_experimento], metrica, ascendente)
        columna = f"metrics.{metrica}"
        if columna in runs.columns:
            runs = runs[runs[columna].notna()]
            if len(runs):
                return runs.iloc[0], metrica
    return None, None
