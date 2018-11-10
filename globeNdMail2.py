from pyspark.sql import SparkSession, Row
from pyspark.sql.functions  import to_date
import collections
import pandas as pd
# sc = spark.sparkContext
# from pyspark.sql import SQLContext
# sqlsc = SQLContext(sc)
# admin = sqlsc.read.parquet('gs://bucket965475263/administration_set.parquet')
spark = SparkSession.builder.config("spark.sql.warehouse.dir").appName('sparkSQL').getOrCreate()


# This blocks gets me the dataFrame for admin
lines = spark.sparkContext.textFile("admin.csv")
def parserLine(line):
    fields = line.split(',')
    return Row(caseID = int(fields[0]), timestamp = pd.to_datetime(int(fields[1]), unit = 's'))
    # return Row(caseID = int(fields[0]), timestamp = int(fields[1]))

admin = lines.map(parserLine)
# for a in admin.collect():
#     print(a)
# df_admin = spark.createDataFrame(admin)
# df_admin.registerTempTable("tbl_admin")
# df_admin.show()



# This blocks gets me the dataFrame for result
lines = spark.sparkContext.textFile("result.csv")
def parserLine(line):
    fields = line.split(',')
    return Row(caseID = int(fields[0]), timestamp_r= pd.to_datetime(int(fields[1]), unit = 's'), score_r = int(fields[2]))

result = lines.map(parserLine)


joined = result.join(admin)
joined1 =joined.map(lambda x: admin[x[0]])
for a in joined1.collect():
    print(a)








# df_result = spark.createDataFrame(result)
# df_result.registerTempTable("tbl_result")
# df_result.show()



#
#
# # # This blocks gets me the dataFrame for update
# lines = spark.sparkContext.textFile("update.csv")
# def parserLine(line):
#     fields = line.split(',')
#     return Row(caseID_u = int(fields[0]), timestamp_u = int(fields[1]), score_u = int(fields[2]))
#
# update = lines.map(parserLine)
# df_update = spark.createDataFrame(update)
# df_update.registerTempTable("tbl_update")
# # df_update.show()
#
#
#
#
#
# # This block merges the three dataFrame based on the caseID key
# # df_merged = df_admin.join(df_result, on = 'caseID', how = 'left')
# # df_merged1 = df_merged.join(df_update, on = ['caseID', 'timestamp_r'], how = 'left').show()
#
#
# df_merge = spark.sql("select a.*, b.timestamp_r, b.score_r, timestamp_r - timestamp as duration \
# from tbl_admin as a left join tbl_result as b on a.caseID = b.caseID")
# df_merge.registerTempTable("tbl_merge")
#
# df_merge1 = spark.sql("select a.*, b.timestamp_u, b.score_u from tbl_merge as a left join tbl_update as b on a.timestamp_r = b.timestamp_u and a.caseID = b.caseID_u")
#
# # df_merge.show()
# df_merge1.show()
#
# import pyspark.sql.functions as func
# from pyspark.sql.types import DateType
# df_date1 = df_merge1.withColumn('timestamp', func.from_unixtime('timestamp').cast(DateType()))
# df_date2 = df_date1.withColumn('timestamp_r', func.from_unixtime('timestamp_r').cast(DateType()))
# df_date3 = df_date2.withColumn('timestamp_u', func.from_unixtime('timestamp_u').cast(DateType()))
# df_date3.show()


spark.stop()


































#
