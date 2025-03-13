import os
import pyspark
from delta import *
from delta.tables import *
import threading

TABLE_PATH = "delta-table"


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

df_reader.show()
