import os
import pyspark
from delta import *
from delta.tables import *
import threading
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

TABLE_PATH = "delta-table-append-only"


def append_to_table(df):
    df.write.format("delta").mode("append").save(TABLE_PATH)


def print_table(message="New record"):
    print(f"=== {message} ===")
    df = spark.read.format("delta").load(TABLE_PATH)
    df.show()


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \


spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Enable auto schema merging, this will allow us to merge the schema automatically in all delta lake tables
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# delete the current delta table
os.system(f"rm -rf {TABLE_PATH}")

# create a new delta TABLE
schema = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
])
data = [("Alice", 34)]
df = spark.createDataFrame(data, schema)
df.write.format("delta").save(TABLE_PATH)

# read the delta table
print_table("Initial Table")

df1 = spark.createDataFrame([("bob", 47)], schema)
df2 = spark.createDataFrame([("bob", 23)], schema)
thread1 = threading.Thread(target=append_to_table, args=(df1,))
thread2 = threading.Thread(target=append_to_table, args=(df2,))
thread1.start()
thread2.start()

thread1.join()
thread2.join()

print_table("After two append operations in parallel")
