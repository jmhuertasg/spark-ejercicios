import os

from pyspark import SparkContext
from operator import add

sc = SparkContext(appName='ejercicio3c')
textoCervantes = os.path.join('data', 'cervantes.txt')
rdd_cervantes = sc.textFile(textoCervantes)
wordsNoSignf = ['el','ella','ello','ellas','ellos','lo','la','las','de','un','uno','una','unos','unas','que','de','y','']

# 1- funcion wordCount
def wordCount(rdd):
    lista = rdd.map(lambda x: (x, 1))
    return lista

# 2- funcion limpiar
def clean(string):
    return string.replace('.','').replace(',','').lower()

# 3 - funcion stopWords
def stopWords(string):
    return string.lower() not in wordsNoSignf

#print("True" if stopWords('ellas') else "False")

# 4 - Del fichero cervantes.txt, mostrar las 10 palabras "semanticamente significativas" que mas aparecen

def top10palabras(rdd):

    mappedWords = rdd.map(lambda x: clean(x))\
               .flatMap(lambda y: y.split(' '))\
               .filter(lambda z: stopWords(z))

    wordCounter = wordCount(mappedWords)

    wordReducer = wordCounter.reduceByKey(add)

    print('wordReducer: %s' % wordReducer.collect())

    orderedWords = wordReducer.sortBy(lambda (x,y): y, ascending=False)
    print('orderedWords: %s' % orderedWords.collect())

    top10Words = orderedWords.take(10)
    print('top10: %s' % top10Words)

    return top10Words

top10palabras(rdd_cervantes)
