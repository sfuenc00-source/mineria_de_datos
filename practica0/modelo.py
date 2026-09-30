import os
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import FloatType, StringType
from pyspark.sql import functions as F

RUTA_CSV = os.path.join(os.path.dirname(__file__), "ibex35_close-2024.csv")

def cargar_datos(spark):
    #Ej1-a: lectura del CSV 
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .csv(RUTA_CSV))

def convertir_tipos(df):
    #Ej1-a: Fecha a tipo date y el resto de columnas a tipo float
    df = df.withColumn("Fecha", to_date(df["Fecha"], "dd/MM/yyyy"))
    for column in df.columns[1:]:
        df = df.withColumn(column, col(f"`{column}`").cast(FloatType()))
    return df

def quitar_sufijos(df):
    #Ej1-b: quitar sufijo ".MC" de los nombres de las columnas
    for column in df.columns:
        df = df.withColumnRenamed(column, column.replace(".MC", ""))
    return df

def eliminar_duplicados(df):
    #Ej2-a: eliminar filas duplicadas
    antes = df.count()
    df = df.dropDuplicates().orderBy("Fecha")
    despues = df.count() 
    eliminadas = antes - despues
    return df, eliminadas

def contar_empresas_unicas(df):
    #Ej2-a: contar número de empresas únicas (columnas)
    return len(df.columns) - 1  # Restar 1 para excluir la columna "Fecha"

def calcular_fechas(df):
    #Ej2-b: calcular fecha mínima, máxima y total de días con información disponible
    min_date = df.agg({"Fecha": "min"}).collect()[0][0]
    max_date = df.agg({"Fecha": "max"}).collect()[0][0]
    total_days = df.select("Fecha").distinct().count()
    return min_date, max_date, total_days

def renombrar_columna(df, old_name, new_name):
    #Ej3: renombrar columna
    return df.withColumnRenamed(old_name, new_name)

def calcular_estadisticas(df):
    #Ej3: calcular media, máximo y mínimo anual para cada empresa
    return df.agg(
        *(
            [F.mean(F.col(f"`{c}`")).alias(f"{c}_Media_anual")
             for c in df.columns[1:]]
            +
            [F.max(F.col(f"`{c}`")).alias(f"{c}_Max_anual")
             for c in df.columns[1:]]
            +
            [F.min(F.col(f"`{c}`")).alias(f"{c}_Min_anual")
             for c in df.columns[1:]]
        )
    )

def anadir_deficiency_notice(df):
    #Ej3: True si el cierre de UNI es menor que 1 (evaluando cada dia por separado)
    return df.withColumn("Deficiency Notice UNI", col("UNI") < 1)

def calcular_variacion_anual(df):
    # Ej4: Calcular la variación anual y clasificar las empresas
    variaciones = {}

    for column in df.columns[1:]:  # Excluir la columna "Fecha"
        df_col = df.filter(col(column).isNotNull())
        valor_inicial = df_col.orderBy("Fecha").first()[column]
        valor_final = df_col.orderBy(col("Fecha").desc()).first()[column]
        variacion = ((valor_final - valor_inicial) / valor_inicial) * 100

        if variacion <= -15:
            clasificacion = "Bajada Fuerte"
        elif -15 < variacion <= -1:
            clasificacion = "Bajada"
        elif -1 < variacion < 1:
            clasificacion = "Neutra"
        elif 1 <= variacion < 15:
            clasificacion = "Subida"
        else:  # variacion >= 15
            clasificacion = "Subida Fuerte"

        variaciones[column] = (variacion, clasificacion)
    return variaciones

def calcular_cuartiles(df):
    # Ej5: Calcular los cuartiles para cada empresa
    cuartiles = {}
    for column in df.columns[1:]:  # Excluir la columna "Fecha"
        q1 = df.approxQuantile(column, [0.25], 0.01)[0]
        q2 = df.approxQuantile(column, [0.50], 0.01)[0]
        q3 = df.approxQuantile(column, [0.75], 0.01)[0]
        cuartiles[column] = (q1, q2, q3)
    return cuartiles

def anadir_cuartiles(df, cuartiles):
    #Ej5: columna "<Empresa>Cuartil" con q1, q2, q3 o q4 para cada sesión
    for column in df.columns[1:]:  # Excluir la columna "Fecha"
        q1, q2, q3 = cuartiles[column]
        valor = col(column)
        df = df.withColumn(
            f"{column}Cuartil",
            F.when(valor.isNull(), F.lit(None).cast("string"))
             .when(valor <= q1, "q1")
             .when(valor <= q2, "q2")
             .when(valor <= q3, "q3")
             .otherwise("q4")
        )
    return df

def empresas_a_cuartiles(df):
    # Ej5: Calcular cuartiles y añadir columnas de cuartiles al DataFrame
    cuartiles = calcular_cuartiles(df)
    df_cuartiles = anadir_cuartiles(df, cuartiles)
    return df_cuartiles


    







