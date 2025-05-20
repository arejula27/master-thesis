import os
from pyspark.sql import SparkSession
import threading

TABLE_PATH = "delta-table-conflicts"


def append_to_table(df):
    df.write.format("delta").mode("append").save(TABLE_PATH)


spark = SparkSession.builder \
    .appName("CreateTable") \
    .getOrCreate()
# Enable auto schema merging, this will allow us to merge the schema automatically in all delta lake tables
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


# delete the current delta table
os.system(f"rm -rf {TABLE_PATH}")

# create a new delta TABLE
df = spark.createDataFrame([("bob", 47), ("li", 23), ("leonard", 51)]).toDF(
    "first_name", "age"
)

df.write.format("delta").save(TABLE_PATH)


# read the delta table
df_reader = spark.read.format("delta").load(TABLE_PATH)
df_reader.show()

# Add a new column
df = spark.createDataFrame([("frank", 68, "usa"), ("jordana", 26, "brasil")]).toDF(
    "first_name", "age", "country"
)

df.write.format("delta").mode("append").save(TABLE_PATH)

# Add a record with
df = spark.createDataFrame([("jose", 65)]).toDF("first_name", "age")
df.write.format("delta").mode("append").save(TABLE_PATH)


# Read the delta table to get the schema
df_reader = spark.read.format("delta").load(TABLE_PATH)
df_reader.show()


# We will try to make two append operations in parallel

columns1 = ["first_name", "age", "country"]
columns2 = ["first_name", "age", "job"]
df1 = spark.createDataFrame([("Tom", 34, "France")], columns1)
df2 = spark.createDataFrame([("jane", 25, "Plumber")], columns2)
thread1 = threading.Thread(target=append_to_table, args=(df1,))
thread2 = threading.Thread(target=append_to_table, args=(df2,))

thread1.start()
thread2.start()

thread1.join()
thread2.join()

# Read the delta table to get the schema
df_reader = spark.read.format("delta").load(TABLE_PATH)
df_reader.show()
