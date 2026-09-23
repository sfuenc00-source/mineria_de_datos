from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *


# Crear una SparkSession
spark_session = (SparkSession.builder
 .appName("IBEX35")
 .getOrCreate())


"""
Ej 1-a
"""

df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat",
"dd/MM/yyyy").csv("ibex35_close-2024.csv")

#df.printSchema() 

df = df.withColumn("Fecha", to_date(df["Fecha"], "dd/MM/yyyy"))

for column in df.columns[1:]:
    df = df.withColumn(column, col(f"`{column}`").cast(FloatType()))

#df.printSchema()
#df.show(6)

"""
Ej 1-b
"""
for column in df.columns:
    new_column_name = column.replace(".MC", "")
    df = df.withColumnRenamed(column, new_column_name)

#df.show(6)


"""
Ej2-a
"""
df_cleaned = df.dropDuplicates()
rows_removed = df.count() - df_cleaned.count()
print(f"Filas eliminadas: {rows_removed}") #4
print(f"Empresas con información disponible: {df_cleaned.columns}") #36


"""
Ej2-b
"""
min_date = df_cleaned.agg({"Fecha": "min"}).collect()[0][0]
max_date = df_cleaned.agg({"Fecha": "max"}).collect()[0][0]
print(f"Fecha inicial: {min_date}") #2024-01-02
print(f"Fecha final: {max_date}") #2024-12-30
print(f"Días con información disponible: {(max_date - min_date).days + 1}") #364

""" 
El resultado es coherente con lo esperado, ya que el periodo cubre casi todo el año 2024. No considero necesario 
buscar datos adicionales, ya que la información disponible es suficiente para el análisis del IBEX35 durante ese año.
"""

"""
Ej3
"""

df_renamed = df_cleaned.withColumnRenamed("Fecha", "Dia")
#df_renamed.show(10)

"""
Calcula y muestra para cada empresa la media anual (Media anual), el valor
máximo anual (Max anual) y el valor mínimo anual (Min anual) dentro del
periodo de datos disponible
"""
from pyspark.sql import functions as F

df_stats = df_renamed.agg(
    *(
        [F.mean(F.col(f"`{c}`")).alias(f"{c}_Media_anual")
         for c in df_renamed.columns[1:]]
        +
        [F.max(F.col(f"`{c}`")).alias(f"{c}_Max_anual")
         for c in df_renamed.columns[1:]]
        +
        [F.min(F.col(f"`{c}`")).alias(f"{c}_Min_anual")
         for c in df_renamed.columns[1:]]
    )
)

df_stats.show(1, truncate=False)