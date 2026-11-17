import sys

from pyspark.sql import functions as F

from spark_session import create_spark_session
from modelo import config, pipeline_ml, busqueda, experimentos, feature_store, tablas_delta
from vista import consola as vista

# Práctica 3, Clase 1: sistema MLOps (pipeline + MLflow + Hyperopt) probado con KMeans.
# Todos los ejercicios usan los datos de la capa gold (Práctica 2): BBVA, Open y Close.


def _datos(spark):
    hechos = tablas_delta.leer_delta(spark, config.RUTA_GOLD_HECHOS)
    return pipeline_ml.cargar_datos(hechos, config.TICKER_P3, config.COLUMNAS_P3)


def _ej1(spark):
    # Ej1
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej1")
    pipeline_ml.fijar_semillas(config.SEMILLA_MODELO)
    datos = _datos(spark)
    vista.mostrar_mensaje(f"Datos: {config.TICKER_P3}, columnas {config.COLUMNAS_P3}, {datos.count()} días")

    # Se prueba un pipeline (VectorAssembler + KMeans) por cada k, sin Hyperopt, y se compara la silhouette
    resultados = busqueda.probar_ks(datos, config.COLUMNAS_P3, config.EXPERIMENTO_EJ1,
                                    config.SEMILLA_MODELO, config.K_MIN, config.K_MAX)
    for r in resultados:
        vista.mostrar_mensaje(f"k={r['k']}: silhouette = {r['silhouette']:.4f}")
    mejor = max(resultados, key=lambda r: r["silhouette"])
    vista.mostrar_mensaje(f"Número de agrupaciones correcto (mayor silhouette): k = {mejor['k']}")

    # Clasificación de datos nuevos con el modelo recuperado de MLflow (el pipeline monta solo el vector)
    modelo = experimentos.cargar_modelo(mejor["run_id"])
    predicciones = modelo.transform(datos)
    vista.mostrar_clusters(pipeline_ml.tamanos_clusters(predicciones), pipeline_ml.centros(modelo))
    nuevos = datos.orderBy(F.col("Date").desc()).limit(5).select(*config.COLUMNAS_P3)
    vista.mostrar_mensaje("Clasificación de 5 datos nuevos (Open, Close) con el modelo recuperado de MLflow:")
    vista.mostrar_filas(pipeline_ml.clasificar(modelo, nuevos, config.COLUMNAS_P3), 5)
    vista.mostrar_comentario(
        "Se monta un Pipeline de MLlib con VectorAssembler (Transformer: junta Open y Close en un vector) y "
        "KMeans (Estimator). Un solo fit entrena todas las etapas y devuelve un PipelineModel, que se guarda en "
        "MLflow como artefacto de la run y se vuelve a cargar para clasificar datos nuevos sin repetir ningún "
        "paso a mano (el propio pipeline monta el vector). Se prueban k = 2 a 8 y se elige el de mayor "
        "silhouette: k = 6 (0,82). La silhouette no crece de forma regular (baja en k = 4 y vuelve a subir), "
        "por eso hay que calcularlas todas y comparar. Como Open y Close son casi iguales cada día, los puntos "
        "caen sobre una diagonal y los clusters son en la práctica tramos de precio de BBVA (centroides de 9,6 "
        "a 24,8 euros) más que tipos de comportamiento diario; no hace falta escalar porque las dos columnas "
        "están en euros. Con la seed fija y los datos en una sola partición, el resultado es el mismo en "
        "cada ejecución.",
        "Documentación de Spark MLlib (Pipeline, KMeans, ClusteringEvaluator) y de MLflow")


def _ej2(spark):
    # Ej2
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej2")
    pipeline_ml.fijar_semillas(config.SEMILLA_MODELO)
    datos = _datos(spark)
    mejor_k, trials = busqueda.buscar_k(
        datos, config.COLUMNAS_P3, config.EXPERIMENTO_KMEANS,
        config.SEMILLA_MODELO, config.SEMILLA_HYPEROPT,
        config.K_MIN, config.K_MAX, config.MAX_EVALS)
    for intento in trials.trials:
        k = int(intento["misc"]["vals"]["k"][0])
        vista.mostrar_mensaje(f"Intento: k={k}, davies_bouldin = {intento['result']['loss']:.4f}")
    vista.mostrar_mensaje(f"Número de agrupaciones seleccionado por Hyperopt: k = {mejor_k}")
    vista.mostrar_comentario(
        "Hyperopt (algoritmo TPE) propone valores de k entre 2 y 8 (hp.quniform, 7 valores) y cada intento "
        "queda como una run de MLflow, con k, columnas y las dos seeds como parámetros y davies_bouldin y "
        "silhouette como métricas. La métrica de la búsqueda es Davies-Bouldin (distinta de la silhouette del "
        "Ej1): mide cuánto se solapan los grupos y menor es mejor, así que se devuelve tal cual como loss "
        "(Spark solo trae la silhouette y la he calculado a mano). Hyperopt elige k = 6 (0,40), el mismo que "
        "la silhouette del Ej1. Hay 10 intentos para 7 valores posibles, así que TPE repite algunos k; como la "
        "seed fija cada entrenamiento, el mismo k da exactamente la misma métrica. Seeds: la del modelo "
        "(seed = 1 en KMeans) fija la inicialización de los centroides en cada entrenamiento; la de Hyperopt "
        "(rstate = 42) fija qué valores de k se proponen y en qué orden. Con las dos fijas, la búsqueda "
        "completa se repite igual.",
        "Documentación de Hyperopt (fmin, hp.quniform, tpe.suggest, rstate) y de MLflow")


def _ej3(spark):
    # Ej3
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej3")
    runs = experimentos.recuperar_runs([config.EXPERIMENTO_KMEANS], "davies_bouldin", ascendente=True)
    vista.mostrar_mensaje(f"Runs en el experimento '{config.EXPERIMENTO_KMEANS}': {len(runs)} "
                          f"(ordenadas por davies_bouldin, de mejor a peor)")
    vista.mostrar_runs(runs, "davies_bouldin")
    vista.mostrar_comentario(
        "recuperar_runs usa mlflow.search_runs con order_by sobre la métrica y devuelve un DataFrame de pandas "
        "con una columna por parámetro (params.*) y por métrica (metrics.*); las runs que no tienen esa "
        "métrica quedan al final. Cada ejecución del Ej2 añade 10 runs más al experimento (ese es el "
        "historial que se quiere conservar), por eso salen runs con el mismo k y la misma métrica.",
        "Documentación de MLflow (search_runs)")


def _ej4(spark):
    # Ej4
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej4")
    pipeline_ml.fijar_semillas(config.SEMILLA_MODELO)
    datos = _datos(spark)
    ruta, version = config.RUTA_FEATURES, config.VERSION_FEATURES

    # Se guardan las features (Ticker, Date, Open, Close) como versión 1 y se vuelven a leer de la tabla
    escrita = feature_store.guardar_version(spark, datos, ruta, version)
    vista.mostrar_mensaje(f"Versión {version} guardada en {ruta}" if escrita else
                          f"La versión {version} ya estaba guardada: no se reescribe (las versiones no se modifican)")
    recuperadas = feature_store.recuperar_version(spark, ruta, version, config.TICKER_P3, config.COLUMNAS_P3)
    vista.mostrar_mensaje(f"Features recuperadas de la versión {version}: {recuperadas.count()} filas")

    # Se entrena con las features originales y con las recuperadas (misma seed y mismo k) y se comparan
    k, semilla = config.K_EJ4, config.SEMILLA_MODELO
    modelo_a, pred_a = pipeline_ml.entrenar(datos, config.COLUMNAS_P3, k, semilla)
    modelo_b, pred_b = pipeline_ml.entrenar(recuperadas, config.COLUMNAS_P3, k, semilla)
    for nombre, modelo, pred in (("originales", modelo_a, pred_a), ("recuperadas v1", modelo_b, pred_b)):
        vista.mostrar_mensaje(f"Features {nombre}: silhouette = {pipeline_ml.silhouette(modelo, pred):.6f}, "
                              f"davies_bouldin = {pipeline_ml.davies_bouldin(modelo, pred):.6f}")
    igual = pipeline_ml.mismo_resultado(modelo_a, pred_a, modelo_b, pred_b)
    vista.mostrar_mensaje(f"¿Mismo resultado (mismo cluster en cada día y mismos centroides)? {'SÍ' if igual else 'NO'}")
    vista.mostrar_comentario(
        "Guardar las features con su versión hace que el entrenamiento no dependa de recalcularlas: el modelo "
        "se puede reproducir meses después leyendo la versión 1 aunque cambie el código que prepara los datos, "
        "se corrija la gold o se añadan días nuevos. Para que dos entrenamientos den lo mismo hacen falta tres "
        "cosas: las mismas features (las da el feature store, y una versión guardada no se modifica), el mismo "
        "código y la misma seed (la de KMeans). También influye el orden y el reparto de los datos, por eso "
        "al recuperar se ordenan por fecha y se dejan en una partición igual que al entrenar el original. "
        "Con todo eso el resultado es idéntico y se puede auditar qué datos usó cada modelo.",
        "Documentación de Databricks Feature Store y de Delta Lake")


def _ej5(spark):
    # Ej5
    # Uso de IA: Claude (Anthropic) me ayudó a escribir y corregir este código.
    vista.titulo("Ej5")
    pipeline_ml.fijar_semillas(config.SEMILLA_MODELO)
    hechos = tablas_delta.leer_delta(spark, config.RUTA_GOLD_HECHOS)
    datos = pipeline_ml.cargar_datos(hechos, config.TICKER_EJ5, config.COLUMNAS_P3)
    vista.mostrar_mensaje(f"Datos: {config.TICKER_EJ5}, columnas {config.COLUMNAS_P3}, {datos.count()} días")

    # Misma búsqueda del Ej2 sobre otra empresa, en su propio experimento
    mejor_k, _ = busqueda.buscar_k(
        datos, config.COLUMNAS_P3, config.EXPERIMENTO_EJ5,
        config.SEMILLA_MODELO, config.SEMILLA_HYPEROPT,
        config.K_MIN, config.K_MAX, config.MAX_EVALS)
    vista.mostrar_mensaje(f"Número de agrupaciones seleccionado por Hyperopt para {config.TICKER_EJ5}: k = {mejor_k}")

    # Experimentos disponibles y la mejor run de cada uno
    experimentos_mlflow = experimentos.listar_experimentos()
    vista.mostrar_mensaje(f"Experimentos disponibles: {experimentos_mlflow}")
    for nombre in experimentos_mlflow:
        run, metrica = experimentos.mejor_run_de(nombre)
        vista.mostrar_mejor_run(nombre, run, metrica)
    vista.mostrar_comentario(
        "Cada experimento agrupa las runs de un mismo algoritmo sobre unos datos concretos (kmeans_BBVA, "
        "kmeans_SAN), así que dentro de uno todas las runs se pueden ordenar y comparar sin mezclar datos "
        "distintos, y entre experimentos se compara el mismo algoritmo, con las mismas seeds y el mismo espacio "
        "de búsqueda, sobre empresas distintas. Aquí Hyperopt elige k = 5 para Santander (Davies-Bouldin 0,40) y "
        "k = 6 para BBVA (0,40): la calidad de la agrupación es casi la misma, pero cambia el número de tramos "
        "de precio. Davies-Bouldin y silhouette son cocientes de distancias, así que no dependen de que una "
        "acción cueste 5 euros y otra 20. El experimento del Ej1 solo guarda la silhouette, por eso su mejor "
        "run se elige con esa métrica (mayor es mejor) y no con Davies-Bouldin, que no se puede comparar con ella.",
        "Documentación de MLflow (experimentos y runs)")


EJERCICIOS = {"ej1": _ej1, "ej2": _ej2, "ej3": _ej3, "ej4": _ej4, "ej5": _ej5}


def ejecutar(*partes):
    # Sin argumentos ejecuta del Ej1 al Ej5 en orden; con argumentos, solo esos (p. ej. "ej2")
    elegidos = [p.lower() for p in partes] or list(EJERCICIOS)
    desconocidos = [p for p in elegidos if p not in EJERCICIOS]
    if desconocidos:
        sys.exit(f"Ejercicio(s) desconocido(s): {desconocidos}. Opciones: {list(EJERCICIOS)}")

    spark = create_spark_session()
    # Son pocos datos (unos 500 días de una empresa): con las 200 particiones de shuffle por defecto
    # cada entrenamiento y cada métrica tardaban minutos. Con 4 es mucho más rápido y da lo mismo.
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    for nombre in elegidos:
        EJERCICIOS[nombre](spark)
    spark.stop()
