def titulo(ejercicio):
    print(ejercicio)

def mostrar_esquema(df):
    df.printSchema()

def mostrar_filas(df, n):
    df.show(n)

def mostrar_filas_eliminadas(df_original, df_limpio):
    filas_eliminadas = df_original.count() - df_limpio.count()
    print(f"Filas eliminadas: {filas_eliminadas}")

def mostrar_fechas(min_date, max_date, total_days):
    print(f"Fecha mínima: {min_date}")
    print(f"Fecha máxima: {max_date}")
    print(f"Días con información disponible: {total_days}")



