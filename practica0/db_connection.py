# Uso de IA: Claude (Anthropic) me ayudó a corregir errores; GitHub Copilot (VS Code) me ayudó con errores de escritura.
# Conexión JDBC con MySQL
import os

# Ruta relativa al .jar del conector (dentro del proyecto, carpeta lib)
JAR_PATH = os.path.join(os.path.dirname(__file__), "lib", "mysql-connector-j-26.7.0.jar")

URL = "jdbc:mysql://localhost:3306/IBEX35?useSSL=false&allowPublicKeyRetrieval=true"
PROPERTIES = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    # La contraseña NO se escribe en el código: se lee de una variable de entorno
    "password": os.environ.get("MYSQL_PASSWORD", "")
}

def guardar_tabla(df, tabla, modo="overwrite"):
    df.write.jdbc(url=URL, table=tabla, mode=modo, properties=PROPERTIES)

def leer_tabla(spark, tabla):
    return spark.read.jdbc(url=URL, table=tabla, properties=PROPERTIES)