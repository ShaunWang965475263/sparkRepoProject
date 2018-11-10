from pyspark.sql import SparkSession, Row
from pyspark.sql.functions  import to_date
import collections
import pandas as pd
import numpy as np
from pyarrow import parquet as pq

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()

sc = spark.sparkContext
from pyspark.sql import SQLContext
# SQLContext.setConf("spark.sql.parquet.compression.codec", "gzip")
sqlContext = SQLContext(sc).setConf("spark.sql.parquet.binaryAsString","true")


import gzip
import shutil

with gzip.open("administration_set.parquet.gzip", 'rb') as f_in:
    with open('administration_set.parquet', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

# with gzip.open('administration_set.parquet.gzip', 'r') as f:
#     print(f.dtypes)
#     for line in f.read():
#         print(f.dtypes)
path = 'c://sparkCourse//response_set.parquet.gzip'
with open(path, 'rb') as f:
  contents = f.read()
  print(contents.dtypes)
# hi = sqlContext.read.parquet(contents)
# hi.show()
