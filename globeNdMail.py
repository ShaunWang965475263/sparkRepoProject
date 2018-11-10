from pyspark.sql import SparkSession, Row
from pyspark.sql.functions  import to_date
import collections
import pandas as pd
import numpy as np
# sc = spark.sparkContext
# from pyspark.sql import SQLContext
# sqlsc = SQLContext(sc)
# admin = sqlsc.read.parquet('gs://bucket965475263/administration_set.parquet')
spark = SparkSession.builder.config("spark.sql.warehouse.dir").appName('sparkSQL').getOrCreate()


# This blocks gets me the dataFrame for admin
lines = spark.sparkContext.textFile("admin.csv")
def parserLine(line):
    fields = line.split(',')
    # return Row(caseID = int(fields[0]), timestamp = pd.to_datetime(int(fields[1]), unit = 's'))
    return Row(caseID = str(fields[0]), timestamp = int(fields[1]))

admin = lines.map(parserLine)
# for a in admin.collect():
#     print(a)
df_admin = spark.createDataFrame(admin)
df_admin.registerTempTable("tbl_admin")
# df_admin.show()




# This blocks gets me the dataFrame for result
lines = spark.sparkContext.textFile("result.csv")
def parserLine(line):
    fields = line.split(',')
    return Row(caseID = str(fields[0]), timestamp_r= int(fields[1]), score_r = int(fields[2]))

result = lines.map(parserLine)
df_result = spark.createDataFrame(result)
df_result.registerTempTable("tbl_result")
# df_result.show()





# # This blocks gets me the dataFrame for update
lines = spark.sparkContext.textFile("update.csv")
def parserLine(line):
    fields = line.split(',')
    return Row(caseID_u = str(fields[0]), timestamp_u = int(fields[1]), score_u = int(fields[2]))

update = lines.map(parserLine)
df_update = spark.createDataFrame(update)
df_update.registerTempTable("tbl_update")
# df_update.show()





# This block merges the three dataFrame based on the caseID key
# df_merged = df_admin.join(df_result, on = 'caseID', how = 'left')
# df_merged1 = df_merged.join(df_update, on = ['caseID', 'timestamp_r'], how = 'left').show()

# Question 1. Duration = timestamp in response_set - timestamp in administration_set
df_merge = spark.sql("select a.*, b.timestamp_r, b.score_r, timestamp_r - timestamp as duration \
from tbl_admin as a left join tbl_result as b on a.caseID = b.caseID")
df_merge.registerTempTable("tbl_merge")

df_merge1 = spark.sql("select a.*, b.timestamp_u, b.score_u from tbl_merge as a left join tbl_update as b on a.timestamp_r = b.timestamp_u and a.caseID = b.caseID_u")

# df_merge.show()
# df_merge1.show()

# Question 4, get schmea into the right format
import pyspark.sql.functions as func
from pyspark.sql.types import DateType
df_date1 = df_merge1.withColumn('administration_date', func.from_unixtime('timestamp').cast(DateType()))
df_date1 = df_date1.withColumnRenamed("timestamp", "administration_time")

# df_date1.show()
df_date1.registerTempTable("tbl_final")

# Question 2, find the case_score using weighted average through grouping.
df_agg = spark.sql("select caseID, administration_date, administration_time, timestamp_r, timestamp_u, score_u, score_r, score_r * duration as case_score from tbl_final")
# df_agg.groupby(['caseID', 'administration_date', 'administration_time', 'timestamp_r', 'timestamp_u', 'score_u']).agg({'case_score': np.mean})
df_agg.registerTempTable("tbl_agg")

df_agg1 = spark.sql("select caseID, administration_date, administration_time, timestamp_r, timestamp_u, score_u, score_r, avg(case_score) as case_score from tbl_agg group by caseID, administration_date, administration_time, timestamp_r, timestamp_u, score_u, score_r")
df_agg1.registerTempTable("tbl_agg1")

# Question 3, rank the response timestamp using row_number() by caseID.
df_agg2 =spark.sql("select row_number() over(partition by caseID order by timestamp_r ASC) as test_iteration, a.* from tbl_agg1 as a")
df_agg2.registerTempTable("tbl_agg2")

# df_agg2.show()

# Question 5, update the parquet file based on the update_set.
update_func = (func.when(func.col('score_u').isNull(), func.col('score_r'))
                .otherwise(func.col('score_u')))
df = df_agg2.withColumn('iteration_score', update_func)
# df.show()


# Continue with Question 4, changing data types.
df_transform = df[['caseID','administration_date', 'administration_time', 'test_iteration', 'timestamp_r', 'iteration_score', 'case_score' ]]
df_transform = df_transform.withColumnRenamed("timestamp_r", "completion_time")
# df_transform1 = df_transform.withColumn('completion_time', func.from_unixtime('completion_time').cast(DateType()))

from pyspark.sql.types import DoubleType, TimestampType
df_transform = df_transform.withColumn("iteration_score", df_transform["iteration_score"].cast(DoubleType()))
df_transform = df_transform.withColumn("case_score", df_transform["case_score"].cast(DoubleType()))
df_transform = df_transform.withColumn("administration_time", df_transform["administration_time"].cast(TimestampType()))
df_transform = df_transform.withColumn("completion_time", df_transform["completion_time"].cast(TimestampType()))

# print(df_transform.dtypes)

# Question 6.
df_transform.show()
# df_transform.coalesce(1).write.partitionBy("caseID").parquet('output.parquet')


spark.stop()


































#
