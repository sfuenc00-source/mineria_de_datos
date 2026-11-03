import importlib
import sys

# Uso: python main.py clase1  |  clase2  |  clase3
#      python main.py clase1 ej2   (repite solo el Ej2)
#      python main.py clase3 ej10  (solo ese ejercicio)
# Sin argumento se ejecuta la clase1.
if __name__ == "__main__":
    nombre = sys.argv[1] if len(sys.argv) > 1 else "clase1"
    try:
        controlador = importlib.import_module(f"controlador.{nombre}")
    except ModuleNotFoundError as error:
        if error.name != f"controlador.{nombre}":
            raise  # falta una librería dentro del controlador: que se vea el error real
        sys.exit(f"No existe la clase '{nombre}'. Prueba con: clase1, clase2 o clase3")
    controlador.ejecutar(*sys.argv[2:])
