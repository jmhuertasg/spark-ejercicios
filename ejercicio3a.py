from pyspark import SparkContext

sc = SparkContext('local')
wordsList = ['cat', 'elephant', 'rat', 'rat', 'cat']
wordsRDD = sc.parallelize(wordsList, 4)

# funcion plural
def plural(palabra):
    return palabra + "s"

# wordsRDD en plural
palabrasPlural = wordsRDD.map(plural)
print palabrasPlural.collect()

# longitud de palabras
longitudPalabra = palabrasPlural.map(lambda x: len(x))
print longitudPalabra.collect()

# transformar en pares de elementos (word,1)
transformadas = palabrasPlural.map(lambda x: (x, 1))
print transformadas.collect()

# contar ocurrencias de palabra ReduceByKey
reducebyK = transformadas.reduceByKey(lambda a, b: a + b)
print("reduceByKey: %s" % reducebyK.collect())

# contar ocurrencias de palabra GroupByKey
groupbyK = transformadas.groupByKey().map(lambda (k,v): (k,sum(v)))
print("groupByKey: %s" % groupbyK.collect())

