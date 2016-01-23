import os
import datetime
import re

from pyspark import SparkContext
from pyspark.sql import Row
from operator import add

sc = SparkContext(appName='ejercicio5')

def pinta(rdd):
    for p in rdd.collect():
        print(p)

def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[6:10]),
                             int(s[3:5]),
                             int(s[0:2]))


def parsePartidosLine(linePartido):
    split = linePartido.split('::')
    return (Row(
        idPartido       = int(split[0]),
        temporada       = split[1],
        jornada         = int(split[2]),
        equipolocal     = split[3],
        equipovisitante = split[4],
        goleslocal      = int(split[5]),
        golesvisitante  = int(split[6]),
        fecha           = parse_apache_time(split[7]),
        timestamp       = split[8]
    ))

def parsePartidos():

    #logPartidos = os.path.join('data', 'DataSetPartidos.reducido200.txt')
    logPartidos = os.path.join('data', 'DataSetPartidos.txt')

    parsed_partidos = (sc.textFile(logPartidos)
                       .map(parsePartidosLine)
                       .cache())

    return parsed_partidos


parsed_partidos = parsePartidos()

# APARTADO 1 - Cuantos goles ha marcado el Real Sporting (con acumulador para partidos con empate a 0-0)
partidos_a_cero = sc.accumulator(0)

def func_goles_Local(golesLocal, golesVisitante):
    global partidos_a_cero
    if (golesLocal + golesVisitante == 0):
        partidos_a_cero += 1
    return golesLocal

def func_goles_Visitante(golesLocal, golesVisitante):
    global partidos_a_cero
    if (golesLocal + golesVisitante == 0):
        partidos_a_cero += 1
    return golesVisitante

def golesMarcados(equipo):
    print '---------------------------------------------'
    print '1 - Estadisticas del %s' % equipo
    print '---------------------------------------------'
    filterLocal = parsed_partidos.filter(lambda x: x.equipolocal==equipo)
    filterVisitante = parsed_partidos.filter(lambda x: x.equipovisitante==equipo)

    golesLocal = filterLocal.map(lambda x: func_goles_Local(x.goleslocal,x.golesvisitante)).reduce(add)
    golesVisitante = filterVisitante.map(lambda x: func_goles_Visitante(x.goleslocal,x.golesvisitante)).reduce(add)
    print 'Goles como local    : %r' % golesLocal
    print 'Goles como visitante: %r' % golesVisitante

    golesTotales = golesLocal + golesVisitante

    return golesTotales

#print 'Goles marcados      : %r' % golesMarcados('Sporting de Gijon')
#print 'Partidos sin goles  : %r' % partidos_a_cero.value

# APARTADO 2 - En que temporada se marcaron mas goles
print '-----------------------------------------------'
print '2 - Temporada con mas goles'
print '-----------------------------------------------'
def golesPartido(glocal, gvisit):
    return glocal+gvisit

def temporadaMasGoleadora():
    tempMasGoles = parsed_partidos.map(lambda x: (x.temporada,golesPartido(x.goleslocal,x.golesvisitante)))\
                                  .reduceByKey(add)
    maximo = tempMasGoles.sortBy(lambda x: x[1], ascending=False).take(1)
    return maximo

#print temporadaMasGoleadora()

# APARTADO 3 - Equipo mas goleador a)como local, b)como visitante
print '-----------------------------------------------'
print '3 - Equipo mas goleador'
print '-----------------------------------------------'
def masGoleadorLocal():
    local = parsed_partidos.map(lambda x: (x.equipolocal,x.goleslocal)).reduceByKey(add)
    maximoGoleadorLocal = local.sortBy(lambda x:x[1],False).take(1)
    return maximoGoleadorLocal

def masGoleadorVisitante():
    local = parsed_partidos.map(lambda x: (x.equipovisitante,x.golesvisitante)).reduceByKey(add)
    maximoGoleadorVisitante = local.sortBy(lambda x:x[1],ascending=False).take(1)
    return maximoGoleadorVisitante

#print 'Local     : %r' % masGoleadorLocal()
#print 'Visitante : %r' % masGoleadorVisitante()

# APARTADO 4 - Cual son las 3 decadas en las q mas goles se metieron
print '-----------------------------------------------'
print '4 - 3 Decadas mas goleadoras'
print '-----------------------------------------------'
def decadasMasGoleadoras():
    golesXdecada = parsed_partidos.map(lambda x:(str(x.fecha.year)[:3], golesPartido(x.goleslocal,x.golesvisitante)))\
                                .reduceByKey(add)
    decadasMasG = golesXdecada.sortBy(lambda x:x[1],ascending=False).take(3)
    return decadasMasG

#print decadasMasGoleadoras()

# APARTADO 5 - Mejor local en los ultimos 5 anyos
print '-----------------------------------------------'
print '5 - Mejor local ultimos 5 anyos'
print '-----------------------------------------------'
def mejorLocalUltimos5anyos():
    mejorLocalFilter = parsed_partidos.filter(lambda x: x.fecha.year >= 2010)
    mejorLocalReduceByKey = mejorLocalFilter.map(lambda x: (x.equipolocal,x.goleslocal))\
                                .reduceByKey(add)
    resMejorLocal = mejorLocalReduceByKey.sortBy(lambda x:x[1],ascending=False).take(1)

    return resMejorLocal

#print mejorLocalUltimos5anyos()

# APARTADO 6 - Media de victorias por temporada de equipos que han estado menos de 10 temporadas en primera
print '-----------------------------------------------'
print '6 - Media victorias por temporada de equipos con menos de 10 temporadas en primera'
print '-----------------------------------------------'
# Devuelve la lista de equipos que llevan menos de 10 temporadas
def func_menosDe10Temporadas():

    temporadasEnPrimera = parsed_partidos.map(lambda x: (x.equipolocal, x.temporada)).distinct()\
            .map(lambda (x,y): (x,1)).reduceByKey(add)

    equiposConMenos10Temporadas = temporadasEnPrimera.filter(lambda (x,y): y<10)

    equipos = equiposConMenos10Temporadas.map(lambda (x,y):x)

    return equipos

#pinta(menosDe10Temporadas())

# Funcion auxiliar que devuelve el resultado del partido (ganado=1, perdido o empatado=0)
def func_partidoGanado(equipo, Elocal, Evisit, Glocal, Gvisit):
    if (equipo == Elocal and Glocal > Gvisit) or (equipo == Evisit and Glocal < Gvisit):
        return 1
    else:
        return 0

# Funcion para sacar la media de victorias por temporada dado un equipo como argumento
def func_victoriasPorTemporada(equipo):

    filterPartidosEquipo = parsed_partidos.filter(lambda x: x.equipolocal==equipo or x.equipovisitante==equipo)
    victoriasPorTemporada = filterPartidosEquipo.map(lambda x: (x.temporada, func_partidoGanado(equipo,x.equipolocal,x.equipovisitante,x.goleslocal,x.golesvisitante)))\
        .reduceByKey(add).sortByKey()
    #print victoriasPorTemporada.collect()
    mediaVictorias = victoriasPorTemporada.map(lambda (x,y): y).mean()
    print equipo, mediaVictorias

# Para probar con un equipo
#print func_victoriasPorTemporada('Barcelona')

# Recorre el array de equipos que llevan menos de 10 anyos y para cada uno calcula la media de victorias
for equipo in func_menosDe10Temporadas().collect():
    func_victoriasPorTemporada(equipo)



