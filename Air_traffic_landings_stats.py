from pyspark.sql import SparkSession
from pyspark.sql import Row
import collections

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "c:/temp").appName("Landing Stats").getOrCreate()
rdd= spark.sparkContext.textFile("Air_Traffic_Landings_Statistics.csv")

def parseLines(line):
    fields = line.split(",")
    return Row(opAir = fields[1], bodyType = fields[8], Manu = fields[9], model = fields[10], landingCount = int(fields[12]), total_landed_weight = int(fields[13]))

rdd1 = rdd.map(parseLines)

stats = spark.createDataFrame(rdd1).cache()
stats.createOrReplaceTempView("stats")

query = spark.sql("select Manu, bodyType,\
sum(total_landed_weight), sum(landingCount) from stats group by Manu, bodyType")

for result in query.collect():
    print(result)

spark.stop()
