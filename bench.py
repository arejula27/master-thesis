
import os
import random
import string
import pyspark
from delta import *
from delta.tables import *
import threading

TABLE_PATH = "delta-table-bench"


def random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


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


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \


spark = configure_spark_with_delta_pip(builder).getOrCreate()


# delete the current delta table
os.system(f"rm -rf {TABLE_PATH}")

# create a new delta TABLE
data = [(random_string(),)]
columns = ["col1"]
df = spark.createDataFrame(data, columns)
df.write.format("delta").save(TABLE_PATH)

# read the delta table
df = spark.read.format("delta").load(TABLE_PATH)
df.show()
