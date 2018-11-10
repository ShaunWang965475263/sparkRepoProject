# # -*- coding: utf-8 -*-
# path = 'c://sparkCourse//administration_set.parquet.gzip'
#
# with open(path, encoding = 'utf-8', errors="ignore") as f:
#     contents = f.read()

from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()
from pyspark.sql import SQLContext
sc = spark.sparkContext
sqlContext = SQLContext(sc)
df = sqlContext.read.parquet('administration_set.parquet.gzip')

df.show()
