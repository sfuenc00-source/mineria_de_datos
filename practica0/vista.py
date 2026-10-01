def titulo(ejercicio):
    print(ejercicio)

def mostrar_esquema(df):
    df.printSchema()

def mostrar_filas(df, n):
    df.show(n)

def mostrar_empresas_unicas(num_empresas):
    print(f"Número de empresas únicas: {num_empresas}")

def mostrar_filas_eliminadas(eliminadas):
    print(f"Filas eliminadas: {eliminadas}")

def mostrar_fechas(min_date, max_date, total_days):
    print(f"Fecha inicial: {min_date}")
    print(f"Fecha final: {max_date}")
    print(f"Días con información disponible: {total_days}")

def mostrar_comentario(comentario, referencia=""):
    print(f"Comentario/Reflexión/Investigación: {comentario}")
    print(f"Referencia: {referencia}")

def mostrar_variacion_anual(dict_variacion):
    for empresa, (variacion, clasificacion) in dict_variacion.items():
        print(f"{empresa}: Variación anual = {variacion:.2f}%, Clasificación = {clasificacion}")

def mostrar_columnas(df, empresas):
    #precio y cuartil de las empresas indicadas
    columnas = []
    for empresa in empresas:
        columnas += [empresa, f"{empresa}Cuartil"]
    df.select(*columnas).show(df.count())

def mostrar_mensaje(mensaje):
    print(mensaje)
