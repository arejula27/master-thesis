import os
import pyspark
from delta import *
from delta.tables import *
import threading

TABLE_PATH = "delta-table-custom"

delta_jar_path = "/home/arejula27/workspaces/delta/target/scala-2.12/delta_2.12-3.3.0.jar"

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.jars", delta_jar_path) \
    .config("spark.driver.extraClassPath", delta_jar_path) \
    .config("spark.executor.extraClassPath", delta_jar_path)


spark = configure_spark_with_delta_pip(builder).getOrCreate()

delta_version = spark.version
print(f"Delta Version: {delta_version}")


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
