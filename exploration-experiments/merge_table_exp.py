from pyspark.sql import SparkSession
import os
import threading
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

TABLE_PATH = "delta-table-exp"


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


def get_folder_stats():
    """Get the number of parquet files in a folder of the table"""

    parquet_files = [file for file in os.listdir(
        TABLE_PATH) if file.endswith('.parquet')]
    return len(parquet_files)


def print_table(message="New record"):
    print(f"=== {message} ===")
    df = spark.read.format("delta").load(TABLE_PATH)
    df.show()
    print(f"Number of parquet files: {get_folder_stats()}")


spark = SparkSession.builder \
    .appName("CreateTable") \
    .getOrCreate()

# Enable auto schema merging, this will allow us to merge the schema automatically in all delta lake tables
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# delete the current delta table
os.system(f"rm -rf {TABLE_PATH}")

# create a new delta TABLE
schema = StructType([
    StructField("name", StringType(), False),
])
data = [("Alice", )]
df = spark.createDataFrame(data, schema)
df.write.format("delta").save(TABLE_PATH)

# read the delta table
print_table("Initial Table")

# Add a new column
new_schema = StructType([
    StructField("name", StringType(), False),
    StructField("country", StringType(), False),
])
data1 = [("frank", "usa")]
df = spark.createDataFrame(data1, new_schema)
df.write.format("delta").mode("append").save(TABLE_PATH)
print_table("New Column")

data = [("jose",)]
df = spark.createDataFrame(data, schema)
df.write.format("delta").mode("append").save(TABLE_PATH)
print_table("New Record")

data = [("Jorge",), ("Maria",)]
df = spark.createDataFrame(data, schema)
df.write.format("delta").mode("append").save(TABLE_PATH)
print_table("New Record")
