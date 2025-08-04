import os
from pyspark.sql import SparkSession
import time
import threading

TABLE_PATH = "delta-table-lineazability"


spark = SparkSession.builder \
    .appName("CreateTable") \
    .getOrCreate()

os.system(f"rm -rf {TABLE_PATH}")


data = [("test",)]
columns = ["col1"]
df = spark.createDataFrame(data, columns)
df.write.format("delta").save(TABLE_PATH)


start_time = time.time()
df_r = spark.read.format("delta").load(TABLE_PATH)
# df_r.show()
end_time = time.time()
print(f"Read time: {end_time-start_time} seconds")
data = [("test2",)]
df = spark.createDataFrame(data, columns)
df.write.format("delta").mode("append").save(TABLE_PATH)
start_time = time.time()
# df_r.show()
end_time = time.time()
print(f"Read time: {end_time-start_time} seconds")

data = [("test3", )]
df = spark.createDataFrame(data, columns)
df.write.format("delta").mode("append").save(TABLE_PATH)
start_time = time.time()
df_r = spark.read.format("delta").load(TABLE_PATH)
# df_r.show()
end_time = time.time()
print(f"Read time: {end_time-start_time} seconds")
