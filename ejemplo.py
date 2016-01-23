from pyspark import SparkContext

sc = SparkContext('local')
words = sc.parallelize(["scala","java","hadoop","spark","akka"])
print words.count()