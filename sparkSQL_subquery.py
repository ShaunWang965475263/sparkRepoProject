from pyspark.sql import SparkSession
from pyspark.sql import Row
import collections
import pandas as pd

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/temp").appName("SparkSqlSubQuery").getOrCreate()



def mapperItem(line):
    fields = line.split("|")
    return Row(mv_id = int(fields[0]), mv_nm = str(fields[1].encode("utf-8")))

lines = spark.sparkContext.textFile("ml-100k/u.item")
item = lines.map(mapperItem)
################
# mv_id | mv_nm
################
def mapperData(line):
    fields = line.split()
    return Row (usr_id = int(fields[0]), mv_id = int(fields[1]), ratings = int(fields[2]), timestamp = pd.to_datetime(int(fields[3]), unit = 's'))

lines = spark.sparkContext.textFile("ml-100k/u.data")
data = lines.map(mapperData)

################
# usr_id | mv_id | ratings | timestamp
################

for datum in data.collect():
    print(datum)

Schemaitem = spark.createDataFrame(item)
# Schemaitem.createOrReplaceTempView('item')
Schemadata = spark.createDataFrame(data)
# Schemadata.createOrReplaceTempView('data')


for result in Schemaitem.join(Schemadata, how='left', on = 'mv_id').collect():
    print(result)




spark.stop()
