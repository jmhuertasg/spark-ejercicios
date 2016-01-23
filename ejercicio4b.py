import re
import datetime
import os

from pyspark.sql import Row
from pyspark import SparkContext
from operator import add

sc = SparkContext('local')

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

def parse_apache_time(s):

    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):

    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """

    # A regular expression pattern to extract fields from the log line

    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)

def parseLogs():

    """ Read and parse log file """
    #logFile = os.path.join('data', 'apache.access.log.reducido.500lines')
    logFile = os.path.join('data', 'apache.access.log.PROJECT')
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()

    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs


parsed_logs, access_logs, failed_logs = parseLogs()


# 1 - Minimo, Maximo y Media del tamanyo de las peticiones (size)
size_pet = access_logs.map(lambda x : x.content_size).cache()
print '1 - Max: %d' % size_pet.max() + ' Min %d' % size_pet.min() + ' Mean %d' % size_pet.mean()

# 2 - Num. de peticiones de cada codigo de respuesta (response_code)
response_code = access_logs.map(lambda x : (x.response_code, 1))\
            .reduceByKey(add)
print '2 - %r' % response_code.collect()

# 3 - Mostrar 20 hosts que han sido visitados mas de 10 veces
hosts_20visits = access_logs.map(lambda x : (x.host, 1))\
                            .reduceByKey(add)\
                            .filter(lambda x: x[1]>10)\
                            .take(20)
print '3 - %r' % hosts_20visits

# 4 - Mostrar los 10 endpoints mas visitados
host_10ends = access_logs.map(lambda x: (x.host, 1)).reduceByKey(add)
top10host = host_10ends.filter(lambda s: s[1] > 10).sortBy(lambda r: r[1], ascending=False)
print '4 - %r' % top10host.collect()

# 5 - Mostrar los 10 endpoints mas visitados que no tienen codigo de respuesta =200
host_10ends_sin200_filter = access_logs.filter(lambda x: x.response_code != 200)
host_10ends_sin200 = host_10ends_sin200_filter.map(lambda x: (x.host, 1)).reduceByKey(add)
top10host = host_10ends_sin200.filter(lambda s: s[1] > 10).sortBy(lambda r: r[1], ascending=False)
print '5 - %r' % top10host.collect()

# 6 - Calcular el num. de hosts distintos
distinctHosts = access_logs.map(lambda x: x.host).distinct().count()

print '6 - %r' % distinctHosts

# 7 - Contar el no de hosts unicos cada dia
hostUnicosXdia_1 = access_logs.map(lambda s: (s.date_time.day, s.host))
hostUnicosXdia_2 = hostUnicosXdia_1.groupByKey()
hostUnicosXdia_3 = hostUnicosXdia_2.map(lambda (x,y): (x,len(set(y))))

print '7 - %r' % hostUnicosXdia_3.collect()

# 8 - Calcular la media de peticiones diarias por host
mediaHostUnicosXdia_4 = hostUnicosXdia_2.map(lambda (x,y): (x,len(y)/len(set(y))))

print '8 - %r' % mediaHostUnicosXdia_4.collect()

# 9 - Mostrar una lista de 40 endpoints distintos que generan codigo de respuesta = 404
hostDist404 = access_logs.filter(lambda s: s.response_code == 404).cache()
list40ends = hostDist404.map(lambda x: x.host).distinct().take(40)
print '9 - %r' % list40ends

# 10 -Mostrar el top 25 de endopoints que mas codigos de respuesta 404 generan
top25ends = hostDist404.map(lambda x: (x.host, 1)).reduceByKey(add).sortBy(lambda x: x[1],ascending=False).take(25)
print '9b - %r' % top25ends

# 11 - El top 5 de dias que se generaron codigo de respuestas 404
top5dias404 = hostDist404.map(lambda x: (x.date_time.day, 1)).reduceByKey(add).sortBy(lambda y: y[1],ascending=False).take(5)
print '11 - %r' % top5dias404
