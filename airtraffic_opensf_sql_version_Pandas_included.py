# from pyspark import SparkConf, SparkContext
# import collections
#
# conf = SparkConf().setMaster('local[*]').setAppName('airPortTraffic')
# sc = SparkContext(conf = conf)
#
#
# def parseLine(lines):
#     fields = lines.split(',')
#     airline = fields[1]
#     acType = fields[7]
#     passengerCount = int(fields[11])
#     return ((airline, acType), passengerCount)
#
#
# rdd = sc.textFile('Air_Traffic_Passenger_Statistics.csv').map(parseLine)
# rdd1 = rdd.reduceByKey(lambda x , y: x+y)
#
#
# # print(rdd.keys())
#
# # results = rdd1.collect()
#
# # for result in results:
# #     print(result)
# #
#
# from pyspark.sql import SparkSession
# from pyspark.sql import Row
#
# spark = SparkSession.builder.config('spark.sql.warehouse.dir','file:///c:/temp').appName('thisSQLversion').getOrCreate()
#
# rdd = spark.sparkContext.textFile('file:///sparkCourse/Air_Traffic_Passenger_Statistics.csv')
#
# def parsedf (lines):
#     fields = lines.split(',')
#     airline = fields[1]
#     acType = fields[7]
#     passengerCount = int(fields[11])
#     return Row(airline = airline, activityType = acType, count = passengerCount)
#
# pre_df = rdd.map(parsedf)
#
# df = spark.createDataFrame(pre_df).cache()
# df.createOrReplaceTempView('df')
#
# query = spark.sql("select airline, activityType, sum(count) as total from df group by airline, activityType order by sum(count)")
#
# # for result in query.collect():
# #     print(result)
#
#
# spark.stop()




# import numpy as np
# # df.groupby(['airline','activityType']).agg({'count': np.sum}).show()
# import pandas as pd
#
# df1 = pd.read_csv("C:\sparkCourse\Air_Traffic_Passenger_Statistics.csv")
# df1.columns = ['0', 'airlines', '2', '3', '4', '5', '6','type','8','9,','10','count']
# df2 = df1[['airlines', 'type', 'count']]
#
# df3 = df2.groupby(['airlines', 'type']).agg({'count': np.sum})
# print(df3.head(100))
