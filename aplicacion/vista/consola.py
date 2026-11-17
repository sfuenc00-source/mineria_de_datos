def titulo(ejercicio):
    print(ejercicio)

def mostrar_esquema(df):
    df.printSchema()

def mostrar_filas(df, n):
    df.show(n)

def mostrar_mensaje(mensaje):
    print(mensaje)

def mostrar_comentario(comentario, referencia=""):
    print(f"Comentario/Reflexión/Investigación: {comentario}")
    print(f"Referencia/s: {referencia}")

def mostrar_nulos(comentarios_nulos):
    for columna, motivo in comentarios_nulos.items():
        print(f"{columna}: {motivo}")

def mostrar_historial(df_historial):
    # Historial de versiones de una tabla Delta (lo más importante, sin cortar el texto)
    df_historial.select("version", "timestamp", "operation").show(truncate=False)


def mostrar_clusters(tamanos, centros):
    # Práctica 3: tamaño y centroide de cada cluster
    for cluster, dias in tamanos.items():
        centro = ", ".join(f"{valor:.2f}" for valor in centros[cluster])
        print(f"Cluster {cluster}: {dias} días, centroide ({centro})")

def _params(fila, columnas_params):
    return ", ".join(f"{c[len('params.'):]}={fila[c]}" for c in columnas_params if fila[c] == fila[c])

def mostrar_runs(runs, metrica):
    # Práctica 3 (Ej3): una línea por run con su id, los parámetros guardados y el valor de la métrica
    columnas_params = [c for c in runs.columns if c.startswith("params.")]
    for _, fila in runs.iterrows():
        valor = fila[f"metrics.{metrica}"]
        print(f"Run ID: {fila['run_id']} | {_params(fila, columnas_params)} | {metrica}: {valor:.4f}")

def mostrar_mejor_run(experimento, run, metrica):
    # Práctica 3 (Ej5): mejor run de un experimento
    if run is None:
        print(f"{experimento}: sin runs con métricas")
        return
    columnas_params = [c for c in run.index if c.startswith("params.")]
    print(f"{experimento} -> mejor run {run['run_id']} | {_params(run, columnas_params)} | "
          f"{metrica}: {run[f'metrics.{metrica}']:.4f}")
