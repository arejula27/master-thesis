import os
from pyspark.sql import SparkSession
import sys
table_name = None
if len(sys.argv) > 1:
    table_name = sys.argv[1]
else:
    exit(1)

spark = SparkSession.builder \
    .appName("CreateTable") \
    .getOrCreate()


# get the path to the delta table from arguments 1
# read the delta table
df = spark.read.format("delta").load(table_name)
df.show()
