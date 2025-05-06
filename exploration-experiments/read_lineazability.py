import os
import pyspark
from delta import *
from delta.tables import *
import time
import threading

TABLE_PATH = "delta-table-lineazability"


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \


spark = configure_spark_with_delta_pip(builder).getOrCreate()


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
