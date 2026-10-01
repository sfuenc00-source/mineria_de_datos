from spark_session import create_spark_session
import db_connection
import modelo
import vista

def ejecutar():
    # Crear SparkSession
    spark = create_spark_session(jar_path=db_connection.JAR_PATH)

    # Ej1-a
    # Uso de IA: Claude (Anthropic) me ayudó a corregir errores; GitHub Copilot (VS Code) me ayudó con errores de escritura.
    vista.titulo("Ej1-a")
    df = modelo.cargar_datos(spark)
    vista.mostrar_esquema(df) #esquema inicial
    df = modelo.convertir_tipos(df)
    df_raw = df  # datos originales (con tipos), para guardarlos en la BD
    vista.mostrar_esquema(df) #esquema después de convertir tipos
    vista.mostrar_filas(df, 6)

    # Ej1-b
    # Uso de IA: Claude (Anthropic) me ayudó a corregir errores; GitHub Copilot (VS Code) me ayudó con errores de escritura.
    vista.titulo("Ej1-b")
    df = modelo.quitar_sufijos(df)
    vista.mostrar_filas(df, 6)

    # Ej2-a
    # Uso de IA: Claude (Anthropic) me ayudó a corregir errores; GitHub Copilot (VS Code) me ayudó con errores de escritura.
    vista.titulo("Ej2-a")
    df_cleaned, eliminadas = modelo.eliminar_duplicados(df)
    vista.mostrar_filas_eliminadas(eliminadas)
    empresas_unicas = modelo.contar_empresas_unicas(df_cleaned)
    vista.mostrar_empresas_unicas(empresas_unicas)

    # Ej2-b
    # Uso de IA: Claude (Anthropic) me ayudó a corregir errores; GitHub Copilot (VS Code) me ayudó con errores de escritura.
    vista.titulo("Ej2-b")
    fecha_min, fecha_max, total_dias = modelo.calcular_fechas(df_cleaned)
    vista.mostrar_fechas(fecha_min, fecha_max, total_dias)
    vista.mostrar_comentario("El resultado es coherente con lo esperado, ya que el periodo cubre casi todo el año 2024.\n No considero necesario buscar datos adicionales, ya que la información disponible es suficiente para el análisis del IBEX35 durante ese año.")

    # Ej3
    # Uso de IA: Claude (Anthropic) me ayudó a corregir errores; GitHub Copilot (VS Code) me ayudó con errores de escritura.
    vista.titulo("Ej3")
    df_renamed = modelo.renombrar_columna(df_cleaned, "Fecha", "Dia")
    vista.mostrar_filas(df_renamed, 10)
    df_stats = modelo.calcular_estadisticas(df_renamed)
    vista.mostrar_filas(df_stats, 1)
    df_deficiency = modelo.anadir_deficiency_notice(df_renamed)
    vista.mostrar_filas(df_deficiency, 100)

    # Ej4
    # Uso de IA: Claude (Anthropic) me ayudó a corregir errores; GitHub Copilot (VS Code) me ayudó con errores de escritura.
    vista.titulo("Ej4")
    df_variacion = modelo.calcular_variacion_anual(df_cleaned)
    vista.mostrar_variacion_anual(df_variacion)

    # Ej5
    # Uso de IA: Claude (Anthropic) me ayudó a corregir errores; GitHub Copilot (VS Code) me ayudó con errores de escritura.
    vista.titulo("Ej5")
    df_cuartiles = modelo.empresas_a_cuartiles(df_cleaned)
    vista.mostrar_filas(df_cuartiles, 1)
    vista.mostrar_columnas(df_cuartiles, ["AENA", "BBVA"])

    # Guardar en la base de datos MySQL (IBEX35)
    # Uso de IA: Claude (Anthropic) me ayudó a corregir errores; GitHub Copilot (VS Code) me ayudó con errores de escritura.
    try:
        db_connection.guardar_tabla(df_raw, "Datos2024")
        db_connection.guardar_tabla(df_renamed, "Datos2024_tratados")
        vista.mostrar_mensaje("Tablas guardadas en la base de datos IBEX35")
    except Exception as e:
        vista.mostrar_mensaje(f"No se ha podido guardar en la base de datos: {e}")

    spark.stop()
