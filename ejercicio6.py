import os
import datetime

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from operator import add

sc = SparkContext(appName="ejercicio6")
sqlContext = SQLContext(sc)

def pinta(rdd):
    for p in rdd.collect():
        print(p)

def parse_apache_time(s):
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

# Inferencia del esquema
schemaMatches = sqlContext.createDataFrame(parsed_partidos)
schemaMatches.registerTempTable("partidos")

# APARTADO 1 - Quien ha estado mas temporadas en 1a Division: Sporting u Oviedo

partidosOviedo = sqlContext.sql("SELECT distinct(temporada) FROM partidos "
                          "WHERE equipolocal='Real Oviedo' ")

partidosSporting = sqlContext.sql("SELECT temporada FROM partidos "
                          "WHERE equipolocal='Sporting de Gijon' ")

temporadasEnPrimeraOviedo = partidosOviedo.map(lambda x: x.temporada).count()

temporadasEnPrimeraSporting = partidosSporting.map(lambda x: x.temporada).distinct().count()

print 'Temporadas en primera division'
print 'REAL OVIEDO - ', temporadasEnPrimeraOviedo
print 'SPORTING DE GIJON - ', temporadasEnPrimeraSporting

# APARTADO 2 - Cual es el record de goles como visitante en una temporada del Oviedo

partidosOviedoVisitante = sqlContext.sql("SELECT temporada, golesvisitante FROM partidos "
                          "WHERE equipovisitante='Real Oviedo' ")

recordGolesOviedo = partidosOviedoVisitante.map(lambda x: (x.temporada, x.golesvisitante))\
                                           .reduceByKey(add)\
                                           .sortBy(lambda (x,y): y, False).take(1)

print recordGolesOviedo

# APARTADO 3 - En que temporada se marcaron mas goles en Asturias

partidosAsturias = sqlContext.sql("SELECT temporada,goleslocal,golesvisitante FROM partidos "
                          "WHERE equipolocal='Real Oviedo' "
                          "OR equipolocal='Sporting de Gijon' ")

golesAsturias = partidosAsturias.map(lambda x: (x.temporada, x.goleslocal + x.golesvisitante))\
                                .reduceByKey(add)\
                                .sortBy(lambda (x,y):y,False).take(1)

print golesAsturias

# Goles marcados y recibidos por el Sporting jugando de local

partidosSporting = sqlContext.sql("SELECT * FROM partidos "
                          "WHERE equipolocal='Sporting de Gijon' ")
filtroSporting = partidosSporting.map(lambda x: (x.goleslocal,x.golesvisitante))

# Goles marcados y recibidos por el Oviedo jugando de local

