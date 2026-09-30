from pyspark.sql import SparkSession

def create_spark_session(app_name="IBEX35", jar_path=None):
    """
    Crea y devuelve una SparkSession.
    """
    # Con ayuda de claude porque fallaba 
    builder = (SparkSession.builder
               .appName(app_name)
               .config("spark.driver.host", "localhost")
               .config("spark.driver.bindAddress", "127.0.0.1"))
    if jar_path:
        builder = builder.config("spark.jars", jar_path)
    return builder.getOrCreate()
