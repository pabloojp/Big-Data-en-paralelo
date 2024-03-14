"""
    Asignatura: Programación paralela.
    Curso: 2023-2024
    Alumno: Jimenez Poyatos, Pablo
    Curso: 4 CC
    Carrera: Grado en Matemáicas.
    Entrega 1: Busqueda en paralelo.
"""

# Bibliotecas importadas

import os
import random

from generaDatos import crear_archivos_paralelo

from multiprocessing import Process, Queue, Event
from time import perf_counter, sleep
from typing import Type, Tuple



# CONDICIONES DATO A DATO DE EJEMPLO: todas toman como parámetro de entrada un dato (ya sea int o str en estos casos)
# *Int
def es_primo(n: int) -> bool:
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            return False
    return True

def primo_217(n: int) -> bool:
    if (n % 1000 == 217) and (es_primo(n)):
        return True
    return False

def menor_10(n: int) -> bool:
    return n < 10

def cifras6(n: int) -> bool:
    return n // 100000 >= 1

# *Str
def mas10_caracteres(cadena: str) -> bool:
    return len(cadena) > 10

def str_acabado_er(palabra: str) -> bool:
    return palabra[-2:] == "er"



# CONDICIONES GENERALES DE EJEMPLO: todas toman como parámetro de entrada una lista de tuplas, con primer elemento el
# dato, y segundo elemento el nombre del archivo en el que se encuentra. Para cualquier condicion que se quiera
# poner, hay que tener en cuenta este factor. Todas estas funciones devuleven una tupla:
# * Primer elemento es una lista de tuplas (dato,nombre_archivo) que son los que cumplen la condición. Por ejemplo, en
# la función numeros2_suma1092434, se devulve la lista con las dos tuplas cuyos datos hacen que su suma sea 1092434.
# * Segundo elemento un booleano que corresponde a si se satisface la condición o no.

# Todas las condiciones que tengan en cuenta al menos a un dato, se coge siempre la última tupla introducida en la
# lista y a partir de ella se hacen todas las comprobaciones. Se escoge este elemento porque para los elementos
# anteriores ya han sido verificadas todas sus opciones y han resultado negativas.

def len7(lista : list) -> Tuple:
    result = []
    booleano = False
    if len(lista) == 7:
        result, booleano = lista, True
    return result, booleano

def numeros2_suma1092434(lista : list) -> Tuple:
    longitud = len(lista)
    ultima_tupla_add = lista[longitud-1]
    ultimo_elemento = ultima_tupla_add[0]
    for i in range(longitud-1):
        sig = lista[i]
        if sig[0] + ultimo_elemento == 1092434:
            result = [sig, ultima_tupla_add]
            return (result,True)
    return ([],False)

def suma3num_5_mod7(lista : list) -> Tuple:
    longitud = len(lista)
    ultima_tupla_add = lista[longitud - 1]
    ultimo_elemento = ultima_tupla_add[0]
    for i in range(longitud - 1):
        for j in range(i + 1, longitud-1):
            prim = lista[i]
            seg = lista[j]
            if (prim[0] + seg[0] + ultimo_elemento) % 7 == 5:
                result = [prim, seg, ultima_tupla_add]
                return (result, True)
    return ([], False)

def ternaPitagorica(lista : list) -> Tuple:
    longitud = len(lista)
    ultima_tupla_add = lista[longitud - 1]
    ultimo_elemento = ultima_tupla_add[0]
    for i in range(longitud - 1):
        for j in range(i + 1, longitud-1):
            prim = lista[i]
            seg = lista[j]
            if (prim[0]**2 + seg[0]**2 == ultimo_elemento**2) or (seg[0]**2 + ultimo_elemento**2 == prim[0]**2 ) or (prim[0]**2 + ultimo_elemento**2 == seg[0]**2 ):
                result = [prim, seg, ultima_tupla_add]
                return (result, True)
    return ([], False)

def numero_2000000(lista : list) -> Tuple:
    for i in lista:
        if i[0] == 2000000:
            return ([i],True)
    return ([],False)




# FUNCIONES PRÁCTICA 1.

def inicializar_archivos(nr_archivos: int, nr_procesos: int, nr_datos_por_archivo: int,
                         dato_min: int, dato_max: int, name_carpeta: str, tipo_datos: Type) -> None:
    """
    Crea archivos en una carpeta con datos aleatorios de un tipo específico.

    Parameters
    ----------
    nr_archivos : int
        El número de archivos a crear.
    nr_procesos : int
        El número de procesos a utilizar para la creación de archivos.
    nr_datos_por_archivo : int
        El número de datos por archivo.
    dato_min : int
        El valor mínimo de los datos aleatorios.
    dato_max : int
        El valor máximo de los datos aleatorios.
    name_carpeta : str
        El nombre de la carpeta en la que se crearán los archivos.
    tipo_datos : Type
        El tipo de datos que se generarán y guardarán en los archivos.

    Returns
    -------
    None
    """

    directorio_actual = os.getcwd()
    archivos_directorio_actual = os.listdir(directorio_actual)
    if name_carpeta not in archivos_directorio_actual:
        print(f"No existía en tu directorio actual ninguna carpeta con el nombre {name_carpeta}.", flush=True)
        print("Creando archivos...", flush=True)
        directorio = os.path.join(directorio_actual, name_carpeta)
        crear_archivos_paralelo(nr_archivos, nr_procesos, nr_datos_por_archivo, dato_min, dato_max, directorio,
                                tipo_datos)
        print("Archivos creados.", flush=True)


def encolar_archivos(q_archivos: Queue, nr_buscadores: int, name_carpeta: str) -> None:
    """
    Coloca los nombres de los archivos de una carpeta en una cola para ser procesados.

    Parameters
    ----------
    q_archivos : Queue
        La cola en la que se colocarán los nombres de los archivos.
    nr_buscadores : int
        El número de buscadores que leerán los nombres de los archivos de la cola.
    name_carpeta : str
        El nombre de la carpeta de la que se leerán los archivos.

    Returns
    -------
    None
    """

    ruta = os.getcwd()
    ruta_carpeta = os.path.join(ruta, name_carpeta)
    archivos = os.listdir(ruta_carpeta)

    for i in archivos:
        ruta_archiv = os.path.join(ruta_carpeta, i)
        q_archivos.put(ruta_archiv)

    for i in range(nr_buscadores):
        q_archivos.put(None)


def creador_paralelismos(tipo: Type, nr_buscadores: int, target: callable, args: Tuple):
    """
    Crea procesos o hilos paralelos.

    Parameters
    ----------
    tipo : Type
        Tipo de proceso o hilo a crear.
    nr_buscadores : int
        Número de buscadores.
    target : callable
        Función a ejecutar en cada proceso o hilo.
    args : Tuple
        Argumentos para la función objetivo.

    Returns
    -------
    lista_procesos : TYPE
        Lista de procesos o hilos creados.
    """

    lista_procesos = []
    for i in range(nr_buscadores):
        p2 = tipo(target=target, args=args)
        p2.start()
        lista_procesos.append(p2)
    return lista_procesos


def cerrar_procesos(lista: list):
    """
    Termina una lista de procesos o hilos.

    Parameters
    ----------
    lista : list
        Lista de procesos o hilos para terminar.

    Returns
    -------
    None
        No hay valor de retorno.
    """

    for trabajador in lista:
        trabajador.join()


def coger_elemento_cola(cola: Queue) -> Type:
    """
    Coge un elemento de una cola. En caso de que este vacia, espera hasta que haya uno disponible.
    Siempre acaba porque todas las colas que se crean meten minimo un None para cada buscador, luego cada buscador
    va a obtener siempre un elemento de la cola.

    Parameters:
        cola (Queue): La cola de la que se coge un elemento.

    Returns:
        Any: El elemento extraido de la cola.
    """

    while True:
        try:
            elemento = cola.get()
            return elemento
        except:
            espera = random.uniform(0.001,0.01)
            sleep(espera)


def comprobar_condicion_global(q_aptos : Queue, evento : Event,
                               condicion_global : callable, nr_buscadores : int) -> list:
    """
    Comprueba una condición global sobre datos recibidos en una cola.

    Esta función revisa los elementos de la cola `q_aptos` en busca de datos que cumplen una cierta condición global
    proporcionada por la función `condicion_global`. Se detiene cuando los buscadores ya han terminado su trabajo o
    cuando se activa el evento.

    Parameters
    ----------
    q_aptos : Queue
        Cola que contiene los datos a ser comprobados.
    evento : Event
        Evento que indica la finalización de la búsqueda.
    condicion_global : callable
        Función que evalúa la condición global sobre los datos.
        Debe aceptar una lista de elementos y devolver un booleano y una lista modificada de elementos.
    nr_buscadores : int
        Número de buscadores que están procesando los archivos de datos.

    Return
    -------
    resultado : list
        Lista de elementos que cumplen la condición global.
    """

    contador = 0                # Cuenta los Nones que ha procesado .get()
    lista = []
    resultado = []
    while contador < nr_buscadores and not evento.is_set():
        elemento = coger_elemento_cola(q_aptos)
        if elemento is None:
            contador += 1
        else:
            lista.append(elemento)
            resultado, booleano = condicion_global(lista)
            if booleano:
                evento.set()
    return resultado


def procesador_datos(q_aptos: Queue, evento: Event,
                     archivo: str, condicion: callable, datos: list[Type]):
    """
    Procesa los datos de un archivo y añade datos aptos a la cola.

    Parameters
    ----------
    q_aptos : Queue
        Cola de archivos con datos aptos.
    evento : Event
        Evento para señalización de finalización.
    archivo : str
        Nombre del archivo que contiene los datos.
    condicion : callable
        Condición para filtrar los datos.
    datos : list[Type]
        Lista de datos del archivo.

    Returns
    -------
    None
        No hay valor de retorno.
    """

    tamano = len(datos)
    contador = 0
    while (contador < tamano) and (not evento.is_set()):
        dato = datos[contador]
        if condicion(dato):
            nombre_arch = archivo.split('\\')[-1]
            q_aptos.put((dato, nombre_arch))
        contador += 1


def buscador(q_archivos: Queue, q_aptos: Queue, evento: Event,
             condicion: callable, tipo_datos: Type) -> None:
    """
    Busca datos que cumplan una cierta condición en archivos específicos.

    Parameters
    ----------
    q_archivos : Queue
        Cola de archivos para buscar.
    q_aptos : Queue
        Cola de archivos con datos que cumplen la condición.
    evento : Event
        Evento para señalización de finalización.
    condicion : callable
        Condición para filtrar los datos.
    tipo_datos : Type
        Tipo de datos.

    Returns
    -------
    None
        No hay valor de retorno.
    """

    archivo = coger_elemento_cola(q_archivos)
    while (archivo is not None) and (not evento.is_set()):
        with open(archivo, "r") as file:
            texto = file.read()
        datos_str = texto.split()
        datos = list(map(tipo_datos, datos_str))

        procesador_datos(q_aptos, evento, archivo, condicion, datos)
        archivo = coger_elemento_cola(q_archivos)

    q_aptos.put(None)


def imprimir_resultados(q_aptos : Queue, resultados : list) -> None:
    """
    Imprime los resultados de la búsqueda de datos.

    Parameters
    ----------
    q_aptos : Queue
        Cola de archivos con datos aptos.

    Returns
    -------
    None
        No hay valor de retorno.
    """

    primera_condicion = q_aptos.qsize()
    encontrados = len(resultados)
    if primera_condicion == 0:
        print("No se ha encontrado ningún dato que cumpla la primera condición dato a dato.")
    elif encontrados == 0:
        print("No se ha encontrado ningún dato que cumpla la segunda condición.")
    else:
        print(f"Los datos que satisfacen tanto la primera condición (dato a dato) como la condición global son:")
        for i in range(len(resultados)):
            result, archivo = resultados[i]
            print(f"* {result}, en el archivo {archivo}.")





def main() -> None:

    # En caso de que en el directorio de este archivo no exista ninguna carpeta con el nombre name_carpeta, la creamos
    # con archivos de datos de tipo tipo_datos. Yo solo tengo implantadas como tipo_datos o bien str o bien int.
    # Usamos la función facilitada en el campus pero paralelizada.

    name_carpeta = 'datos'
    tipo_datos = int
    nr_archivos = 20
    nr_procesos = 6
    nr_datos_por_archivo = 1_000_000
    dato_min = 0              # En caso de que el tipo de datos sea str, dato_min y dato_max es el tamaño minimo y
    dato_max = 1_000_000      # maximo que tendran los strings.
    inicializar_archivos(nr_archivos, nr_procesos, nr_datos_por_archivo, dato_min, dato_max, name_carpeta, tipo_datos)



    # Parametros para analizar los datos de los archivos generados anteriormente. Hay que especificar el tipo de datos
    # de los archivos en la variable anterior tipo_datos.
    nr_buscadores = 7
    condicion_dato_dato = primo_217
    condicion_global = len7



    q_aptos = Queue()
    q_archivos = Queue()
    evento = Event()

    archivos = creador_paralelismos(Process, 1, encolar_archivos, (q_archivos, nr_buscadores, name_carpeta))
    buscadores = creador_paralelismos(Process, nr_buscadores, buscador,
        (q_archivos, q_aptos, evento, condicion_dato_dato, tipo_datos))
    resultados = comprobar_condicion_global(q_aptos, evento, condicion_global, nr_buscadores)

    cerrar_procesos(archivos)
    cerrar_procesos(buscadores)
    evento.clear()

    imprimir_resultados(q_aptos, resultados)


if __name__ == '__main__':
    t1 = perf_counter()
    main()
    t2 = perf_counter()

    print(f"Ha tardado {t2 - t1} segundos en procesar los archivos.")

