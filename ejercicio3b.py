from pyspark import SparkContext

sc = SparkContext('local')
wordsList = ['cat', 'elephant', 'rat', 'rat', 'cat']
wordsRDD = sc.parallelize(wordsList, 4)

# Ejercicio 3b
# contar palbras unicas
unicas = wordsRDD.distinct()
print ('Palabras unicas: %d') % unicas.count()

# calcular media de aparicion
mapedKV = wordsRDD.map(lambda x: (x, 1))
reducebyK = mapedKV.reduceByKey(lambda a, b: a + b)
print ('ReduceByKey: %s' % reducebyK.collect())

apariciones = reducebyK.map(lambda (x,y): y)
print ('apariciones: %s' % apariciones.collect())

mean = apariciones.mean()
print ('media: %f' % mean)