from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

import pyspark.sql.functions as func
from pyspark.sql.types import DateType
from pyspark.sql.types import DoubleType, TimestampType


# This blocks gets me the dataFrame for admin
df_admin = sqlContext.read.parquet('administration_set.parquet.gzip')
df_admin.registerTempTable("tbl_admin")
df_admin.withColumn("admin_time", df_admin["admin_time"].cast(TimestampType()))
# df_admin.show()




# This blocks gets me the dataFrame for result
df_result = sqlContext.read.parquet('response_set.parquet.gzip')
df_result.registerTempTable("tbl_result")
df_result.withColumn("complete_time", df_result["complete_time"].cast(TimestampType()))
# df_result.show()





# # This blocks gets me the dataFrame for update
df_update = sqlContext.read.parquet('update_set.parquet.gzip')
df_update.registerTempTable("tbl_update")
# df_update.show()






# Question 1. Duration = timestamp in response_set - timestamp in administration_set
timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
timeDiff = (func.unix_timestamp('complete_time', format=timeFmt)
            - func.unix_timestamp('admin_time', format=timeFmt))
df_merge = spark.sql("select a.case_id, a.admin_time, b.complete_time, b.score from tbl_admin as a left join tbl_result as b on a.case_id = b.case_id")
df_merge = df_merge.withColumn("Duration", timeDiff)
df_merge.registerTempTable("tbl_merge")
# df_merge.write.csv('Question1.csv')
print("Question 1 dataset result:")
df_merge.show()
df_merge1 = spark.sql("select a.*, b.complete_time as complete_time_u, b.score as score_u from tbl_merge as a left join tbl_update as b on a.case_id = b.case_id and a.complete_time = b.complete_time")
# df_merge1.show(100)




# Question 4, get schmea into the right format

df_date1 = df_merge1.withColumn('administration_date', func.from_unixtime(func.unix_timestamp('admin_time')).cast(DateType()))
df_date1 = df_date1.withColumnRenamed("admin_time", "administration_time")
df_date1.registerTempTable("tbl_final")
# df_date1.show()




# Question 2, find the case_score using weighted average through grouping.
df_agg = spark.sql("select case_id, administration_date, administration_time, complete_time, complete_time_u, score, score_u, score * duration as case_score from tbl_final")
df_agg.registerTempTable("tbl_agg")
df_agg1 = spark.sql("select case_id, avg(case_score) as case_score from tbl_agg group by case_id")
df_agg1.registerTempTable("tbl_agg1")
# df_agg1.write.csv('Question2.csv')
print("Question 2 dataset result:")
df_agg1.show()




# Question 3, rank the response timestamp using row_number() by caseID.
df_agg2 =spark.sql("select row_number() over(partition by case_id order by complete_time ASC) as test_iteration, a.* from tbl_agg as a")
df_agg2.registerTempTable("tbl_agg2")
# df_agg2.write.csv('Question3.csv')
print("Question 3 dataset result:")
df_agg2.show()

# Question 5, update the parquet file based on the update_set.
update_func = (func.when(func.col('score_U').isNull(), func.col('score'))
                .otherwise(func.col('score_u')))
df = df_agg2.withColumn('iteration_score', update_func)
df_output = df[['case_id', 'complete_time', 'score']]
# df.write.csv('Question5.csv')
# df_output.write.parquet("response_set.parquet")
print("Question 5 dataset result:")
df.show()


# Continue with Question 4, changing data types.
df_transform = df[['case_id','administration_date', 'administration_time', 'test_iteration', 'complete_time', 'iteration_score' ]]
df_transform.registerTempTable("df_transform")
df_transform1 = spark.sql('select b.*, case_score from tbl_agg1 as a left join df_transform as b on a.case_id = b.case_id')
df_transform1 = df_transform1.withColumn("iteration_score", df_transform1["iteration_score"].cast(DoubleType()))
df_transform1 = df_transform1.withColumn("case_score", df_transform1["case_score"].cast(DoubleType()))
df_transform1 = df_transform1.withColumn("administration_time", df_transform1["administration_time"].cast(TimestampType()))
df_transform1 = df_transform1.withColumn("complete_time", df_transform1["complete_time"].cast(TimestampType()))
# df_transform.write.csv('Question4.csv')
print("Question 4 dataset result:")
df_transform1.show()
# print(df_transform.dtypes)

# Question 6.

df_transform1 = df_transform1.withColumn('complete_time', func.from_unixtime(func.unix_timestamp('complete_time')).cast(DateType()))
df_transform1.show()
print("Question 6 is to be written to parquet files partitioned by complete_time in date format.")
df_transform1.coalesce(1).write.partitionBy("complete_time").parquet('output.parquet')


spark.stop()


































#
