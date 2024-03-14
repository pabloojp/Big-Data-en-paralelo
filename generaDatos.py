from random import randint, choice
from os import path, makedirs
from math import ceil, floor
import multiprocessing as mp
from time import perf_counter
from typing import Type


def generar_palabra(longitud):
    """
    Genera una palabra aleatoria con la longitud especificada.

    Parameters
    ----------
    longitud : int
        Longitud de la palabra a generar.

    Returns
    -------
    str
        Palabra generada aleatoriamente.
    """

    consonantes = 'bcdfghjklmnpqrstvwxyz'
    vocales = 'aeiou'
    palabra = ''
    for i in range(longitud):
        if i % 2 == 0:
            palabra += choice(consonantes)
        else:
            palabra += choice(vocales)
    return palabra


def reparte(numero: int, nr_partes: int) -> list[int]:
    """
    Divide un número en nr_partes partes aproximadamente iguales.

    Parameters
    ----------
    numero : int
        El numero que se va a dividir.
    nr_partes : int
        El numero de partes en las que se divide el numero.

    Returns
    -------
    list[int]
        Una lista de enteros que representan las partes en las que se ha dividido el numero.
    """

    cociente, resto = divmod(numero, nr_partes)
    return [cociente + 1] * resto + [cociente] * (nr_partes - resto)

def suma_acumulada(lista:list) -> list:
    """
    Calcula la suma acumulada de una lista de números.

    Parameters
    ----------
    lista : list
        Lista de números.

    Returns
    -------
    list[int]
        Lista con la suma acumulada de los números.
    """

    result = [0]
    acumul = 0
    for i in lista:
        acumul += i
        result.append(acumul)
    return result

def crear_archivos_de_datos(nr_archivos: int, nr_datos_por_archivo: int, dato_min: int, dato_max: int, directorio: str, tipo_datos: Type, contador:int) -> None:
    """
    Crea archivos con datos aleatorios.

    Parameters
    ----------
    nr_archivos : int
        Número de archivos a crear.
    nr_datos_por_archivo : int
        Número de datos por archivo.
    dato_min : int
        Valor mínimo de los datos.
    dato_max : int
        Valor máximo de los datos.
    directorio : str
        Directorio donde se guardarán los archivos.
    tipo_datos : Type
        Tipo de datos a generar (int o str).
    contador : int
        Contador para el nombre de los archivos.

    Returns
    -------
    None
        No devuelve nada.
    """

    if not path.exists(directorio):
        makedirs(directorio)
    for i in range(nr_archivos):
        nr_datos = randint(ceil(0.9 * nr_datos_por_archivo),floor(1.1 * nr_datos_por_archivo))
        nombre_archivo = f'datos{contador}'
        contador += 1
        ruta_archivo = path.join(directorio, nombre_archivo)
        with open(ruta_archivo, 'w') as archivo:
            if tipo_datos == int:
                datos = [str(randint(dato_min, dato_max)) for _ in range(nr_datos)]
            elif tipo_datos == str:
                datos = [generar_palabra(randint(dato_min, dato_max)) for _ in range(nr_datos)]
            archivo.write(' '.join(datos))
        
            
def crear_archivos_paralelo(nr_archivos: int, nr_procesos: int, nr_datos_por_archivo: int, dato_min: int, dato_max: int, directorio: str, tipo_datos: Type):
    """
    Crea archivos de datos de forma paralela.

    Parameters
    ----------
    nr_archivos : int
        Número de archivos a crear.
    nr_procesos : int
        Número de procesos a utilizar.
    nr_datos_por_archivo : int
        Número de datos por archivo.
    dato_min : int
        Valor mínimo de los datos.
    dato_max : int
        Valor máximo de los datos.
    directorio : str
        Directorio donde se guardarán los archivos.
    tipo_datos : Type
        Tipo de datos a generar (int o str).

    Returns
    -------
    None
        No devuelve nada.
    """

    p = mp.Pool(nr_procesos)
    lista = reparte(nr_archivos, nr_procesos)
    lista_nombre = suma_acumulada(lista)
    pares = list(zip(lista,lista_nombre))
    args_list = [(k[0],nr_datos_por_archivo, dato_min, dato_max, directorio, tipo_datos, k[1]) for k in pares]
    p.starmap(crear_archivos_de_datos, args_list)    



if __name__ == "__main__":
    # Parámetros
    nr_archivos = 10
    nr_datos_por_archivo = 10
    dato_min = 1
    dato_max = 100
    nr_procesos = 6
    tipo_datos = int
    directorio = "../Entrega1/datos"   # Ruta hasta la Hoja 2 y despues donde quiero guardarlo

    # Ejecutar la función para generar los archivos
    
    t1 = perf_counter()  # Inicio del tiempo
    crear_archivos_paralelo(nr_archivos, nr_procesos, nr_datos_por_archivo, dato_min, dato_max, directorio, tipo_datos)
    t2 = perf_counter()  # Fin del tiempo
    
    tiempo_transcurrido = t2 - t1
    print(f"Tiempo transcurrido: {tiempo_transcurrido // 60} minutos y {tiempo_transcurrido % 60} segundos")
    print("")
    
    