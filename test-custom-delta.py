import os
from pyspark.sql import SparkSession

TABLE_PATH = "delta-table-custom"

spark = SparkSession.builder \
    .appName("DeltaExperiment") \
    .getOrCreate()

# delete the current delta table
os.system(f"rm -rf {TABLE_PATH}")

# create a new delta TABLE
data = [("Alice", 34), ("Bob", 56), ("Charlie", 45)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)
df.write.format("delta").save(TABLE_PATH)

# read the delta table
df = spark.read.format("delta").load(TABLE_PATH)
df.show()
