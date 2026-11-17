# Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
# Búsqueda automática del mejor k de KMeans con Hyperopt y MLflow (Práctica 3, Ej2)
import mlflow
import numpy as np
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe

from modelo import experimentos, pipeline_ml


def buscar_k(datos, columnas, nombre_experimento, semilla_modelo, semilla_hyperopt,
             k_min, k_max, max_evals, escalar=False):
    # Prueba valores de k con Hyperopt (TPE) y registra cada intento como una run de MLflow.
    # Devuelve (mejor k, Trials de Hyperopt).
    experimentos.configurar(nombre_experimento)
    datos = datos.cache()  # se entrena varias veces sobre los mismos datos

    # hp.quniform: rango numérico con paso fijo (k es ordinal: 3 está "entre" 2 y 4). Devuelve float.
    espacio = {"k": hp.quniform("k", k_min, k_max, 1)}

    def objetivo(parametros):
        k = int(parametros["k"])
        with mlflow.start_run():
            modelo, predicciones = pipeline_ml.entrenar(datos, columnas, k, semilla_modelo, escalar)
            db = pipeline_ml.davies_bouldin(modelo, predicciones)
            sil = pipeline_ml.silhouette(modelo, predicciones)
            experimentos.registrar_intento(
                parametros={"algoritmo": "KMeans", "k": k, "columnas": ",".join(columnas),
                            "escalar": escalar, "seed_modelo": semilla_modelo,
                            "seed_hyperopt": semilla_hyperopt},
                metricas={"davies_bouldin": db, "silhouette": sil},
                modelo=modelo)
        # Hyperopt MINIMIZA el loss. Davies-Bouldin ya es "menor es mejor", así que se devuelve tal cual
        # (con una métrica "mayor es mejor", como la silhouette, habría que devolver -métrica).
        return {"loss": db, "status": STATUS_OK}

    trials = Trials()
    mejor = fmin(
        fn=objetivo,
        space=espacio,
        algo=tpe.suggest,
        max_evals=max_evals,
        trials=trials,
        # rstate: semilla de Hyperopt. Fija QUÉ valores de k propone y en qué orden (el azar de la búsqueda).
        # La semilla del modelo (seed de KMeans) fija en cambio cada entrenamiento (los centroides iniciales).
        rstate=np.random.default_rng(semilla_hyperopt),
        show_progressbar=False,
    )
    return int(mejor["k"]), trials


def probar_ks(datos, columnas, nombre_experimento, semilla_modelo, k_min, k_max, escalar=False):
    # Ej1 (sin Hyperopt): entrena un pipeline por cada k de la lista, uno por uno, y registra cada
    # entrenamiento como una run de MLflow. Devuelve una lista de dicts con k, silhouette y run_id.
    experimentos.configurar(nombre_experimento)
    datos = datos.cache()
    resultados = []
    for k in range(k_min, k_max + 1):
        with mlflow.start_run() as run:
            modelo, predicciones = pipeline_ml.entrenar(datos, columnas, k, semilla_modelo, escalar)
            sil = pipeline_ml.silhouette(modelo, predicciones)
            experimentos.registrar_intento(
                parametros={"algoritmo": "KMeans", "k": k, "columnas": ",".join(columnas),
                            "escalar": escalar, "seed_modelo": semilla_modelo},
                metricas={"silhouette": sil},
                modelo=modelo)
        resultados.append({"k": k, "silhouette": sil, "run_id": run.info.run_id})
    return resultados
