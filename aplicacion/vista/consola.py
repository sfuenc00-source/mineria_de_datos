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
