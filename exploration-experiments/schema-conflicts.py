import os
import pyspark
from delta import *
from delta.tables import *
import threading

TABLE_PATH = "delta-table"


def append_to_table(df):
    df.write.format("delta").mode("append").save(TABLE_PATH)


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \


spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Enable auto schema merging, this will allow us to merge the schema automatically in all delta lake tables
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


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


# This would show the new records without the new schema
# df_reader.show()


# Read the delta table to get the schema
df_reader = spark.read.format("delta").load(TABLE_PATH)
df_reader.show()


# We will try to make two append operations in parallel

columns = ["first_name", "age", "country"]
df1 = spark.createDataFrame([("Tom", 34, "France")], columns)
df2 = spark.createDataFrame([("jane", 25, "Canada")], columns)
thread1 = threading.Thread(target=append_to_table, args=(df1,))
thread2 = threading.Thread(target=append_to_table, args=(df2,))

thread1.start()
thread2.start()

thread1.join()
thread2.join()

# Show the table
df_reader.show()


# Now lets try to append different schemas that are already in the table
columns1 = ["first_name", "age", "country"]
columns2 = ["first_name", "age"]
df1 = spark.createDataFrame([("Marcos", 34, "France")], columns1)
df2 = spark.createDataFrame([("Ignacio", 25)], columns2)
thread1 = threading.Thread(target=append_to_table, args=(df1,))
thread2 = threading.Thread(target=append_to_table, args=(df2,))

thread1.start()
thread2.start()

thread1.join()
thread2.join()

# Show the table
df_reader.show()


# Now lets try to append different columns
columns1 = ["name", "id"]
columns2 = ["first_name", "city"]
df1 = spark.createDataFrame([("Eli", 24929229)], columns1)
df2 = spark.createDataFrame([("Jorge", "Madrid")], columns2)

thread1 = threading.Thread(target=append_to_table, args=(df1,))
thread2 = threading.Thread(target=append_to_table, args=(df2,))
thread1.start()
thread2.start()

thread1.join()
thread2.join()

# Show the table with the new schema
df_reader = spark.read.format("delta").load(TABLE_PATH)
df_reader.show()
