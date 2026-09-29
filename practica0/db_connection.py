#conexion JDBC 
import os

URL = "jdbc:postgresql://localhost/ibex35"
PROPERTIES = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": os.environ.get("MYSQL_PASSWORD", "")
}

def guardar_tabla(df, tabla, modo="overwrite"):
    df.write.jdbc(url=URL, table=tabla, mode=modo, properties=PROPERTIES)

def leer_tabla(spark, tabla):
    return spark.read.jdbc(url=URL, table=tabla, properties=PROPERTIES)
