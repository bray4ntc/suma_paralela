import mysql.connector
import threading
import time
import math  # Importamos math para usar ceil

# Lista global para almacenar los resultados parciales de cada hilo
resultados_parciales = []
lock = threading.Lock()  # Bloqueo para manejar el acceso a los resultados parciales de manera segura

# Función que se conecta a MySQL, consulta un bloque de datos, suma los valores y mide el tiempo
def sumar_parcial(desde, hasta, hilo_id):
    try:
        # Registrar el inicio del tiempo
        inicio = time.time()

        # Conectar a la base de datos MySQL
        conexion = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="suma_paralela"
        )
        cursor = conexion.cursor()

        # Consultar los valores en el rango especificado
        cursor.execute(f"SELECT valor FROM numeros LIMIT {desde}, {hasta}")
        resultados = cursor.fetchall()

        # Calcular la suma parcial
        suma = sum([fila[0] for fila in resultados])

        # Asegurarse de que el acceso a la lista sea seguro entre hilos
        with lock:
            resultados_parciales.append(suma)

        # Cerrar la conexión
        cursor.close()
        conexion.close()

        # Registrar el fin del tiempo
        fin = time.time()

        # Mostrar la suma parcial y el tiempo que tomó el hilo
        print(f"Hilo {hilo_id}: Suma parcial = {suma}, Tiempo de ejecución = {fin - inicio:.4f} segundos")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

if __name__ == "__main__":
    # Número de hilos que vamos a usar
    num_hilos = 4

    # Total de registros en la tabla (deberías obtener esto con una consulta COUNT)
    total_registros = 22  # Cambié a 102 para que no sea divisible por 4

    # Calcular el tamaño de bloque para cada hilo, redondeado hacia arriba
    tamaño_bloque = math.ceil(total_registros / num_hilos)

    # Crear una lista para almacenar los hilos
    hilos = []

    # Crear e iniciar los hilos
    for i in range(num_hilos):
        desde = i * tamaño_bloque
        hasta = tamaño_bloque
        hilo = threading.Thread(target=sumar_parcial, args=(desde, hasta, i+1))  # Pasamos el ID del hilo para identificarlo
        hilos.append(hilo)
        hilo.start()

    # Esperar a que todos los hilos terminen
    for hilo in hilos:
        hilo.join()

    # Sumar los resultados parciales
    suma_total = sum(resultados_parciales)
    print(f"La suma total es: {suma_total}")
