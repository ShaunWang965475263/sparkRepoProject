from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster('local').setAppName('airPortTraffic')
sc = SparkContext(conf = conf)


def parseLine(lines):
    fields = lines.split(',')
    airline = fields[1]
    acType = fields[7]
    passengerCount = int(fields[11])
    return ((airline, acType), passengerCount)


rdd = sc.textFile('Air_Traffic_Passenger_Statistics.csv').map(parseLine)
rdd1 = rdd.reduceByKey(lambda x , y: x+y)


# print(rdd.keys())

results = rdd1.collect()

for result in results:
    print(result)
