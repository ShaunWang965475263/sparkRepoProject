import pandas as pd
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local[*]').setAppName('FindoutBusinessTypeByZipCode')
sc = SparkContext(conf = conf)

rdd = sc.textFile("file:///sparkCourse/Registered_Business_Locations_-_San_Francisco.csv")
# df = pd.read_csv("Registered_Business_Locations_-_San_Francisco.csv", encoding = 'ascii')
# df.head()

def parseLine(line):
    fields = line.split(',')
    # print(fields)
    zipCode =fields[7]
    bus_type = fields[17]
    return (zipCode, bus_type)


keyValuePair = rdd.map(parseLine)
# results = keyValuePair.collect()
# for result in results:
#     print(result)
keyValuePair1 = keyValuePair.mapValues(lambda x: (1,x)) #MapValues mapValues is only applicable for PairRDDs, meaning RDDs of the form RDD[(A, B)]. In that case, mapValues operates on the value only (the second part of the tuple), while map operates on the entire record (tuple of key and value).


typeByZipCode = keyValuePair1.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
Flipped = typeByZipCode.map(lambda x: (x[1],x[0]))
Fillped1 = Flipped.sortByKey(lambda x:(x[1], x[0]))


results = typeByZipCode.collect()
# for i in range(1,30):
#     print(results[i])
for result in results:
    print(result)

# df = pd.DataFrame(results)
# print(df.head())
