import os
from pyspark.sql import SparkSession
import threading

TABLE_PATH = "delta-table"


def change_schema(old_column, new_column):
    # Change the schema of the delta table
    df = spark.read.format("delta").load(TABLE_PATH)
    df = df.withColumnRenamed(old_column, new_column)
    try:
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true").save(TABLE_PATH)
    except Exception as e:
        print("=== Error ===")
        print(f"Can not change the schema of the column {
              old_column} to {new_column}")
        print(e)
        print("==============")


spark = SparkSession.builder \
    .appName("CreateTable") \
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


thread1 = threading.Thread(target=change_schema, args=("name", "first_name"))
thread2 = threading.Thread(target=change_schema, args=("name", "full_name"))

# Start both threads
thread1.start()
thread2.start()

# Wait for both threads to finish
thread1.join()
thread2.join()

# read the delta TABLE_PATH
df = spark.read.format("delta").load(TABLE_PATH)
df.show()

# get column names
columns = df.columns

thread1 = threading.Thread(
    target=change_schema, args=(columns[0], "user_name"))
thread2 = threading.Thread(target=change_schema, args=(columns[1], "score"))

# Start both threads
thread1.start()
thread2.start()

# Wait for both threads to finish
thread1.join()
thread2.join()

# read the delta TABLE_PATH
df = spark.read.format("delta").load(TABLE_PATH)
df.show()
