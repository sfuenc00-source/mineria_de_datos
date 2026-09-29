from spark_session import create_spark_session
import modelo
import vista

def ejecutar():
    # Crear SparkSession
    spark = create_spark_session()

    # Ejercicio 1-a
    vista.titulo("Ej1-a")
    df = modelo.cargar_datos(spark)
    vista.mostrar_esquema(df) #esquema inicial
    df = modelo.convertir_tipos(df)
    vista.mostrar_esquema(df) #esquema después de convertir tipos
    vista.mostrar_filas(df, 6)

    # Ejercicio 1-b
    vista.titulo("Ej1-b")
    df = modelo.quitar_sufijos(df)
    vista.mostrar_filas(df, 6)

    # Ejercicio 2-a
    vista.titulo("Ej2-a")
    df_cleaned = modelo.eliminar_duplicados(df)
    vista.mostrar_filas_eliminadas(df, df_cleaned)

    # Ejercicio 2-b
    vista.titulo("Ej2-b")
    min, max, total = modelo.calcular_fechas(df_cleaned)
    vista.mostrar_fechas(min, max, total)
    """ 
    El resultado es coherente con lo esperado, ya que el periodo cubre casi todo el año 2024. No considero necesario 
    buscar datos adicionales, ya que la información disponible es suficiente para el análisis del IBEX35 durante ese año.
    """

    # Ejercicio 3
    vista.titulo("Ej3")
    df_renamed = modelo.renombrar_columna(df_cleaned, "Fecha", "Dia")
    vista.mostrar_filas(df_renamed, 10)
    df_stats = modelo.calcular_estadisticas(df_renamed)
    vista.mostrar_filas(df_stats, 1)

    spark.stop()