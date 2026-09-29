from pyspark.sql import SparkSession

def create_spark_session(app_name="IBEX35", jar_path=None):
    """
    Crea y devuelve una SparkSession.
    """
    builder = SparkSession.builder.appName(app_name)
    if jar_path:
        builder = builder.config("spark.jars", jar_path)
    return builder.getOrCreate()
