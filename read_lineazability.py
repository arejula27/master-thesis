import os
import pyspark
from delta import *
from delta.tables import *
import threading

TABLE_PATH = "delta-table"


def read_table():
    df_reader = spark.read.format("delta").load(TABLE_PATH)
    df_reader.show()


def append_to_table(df):
    df.write.format("delta").mode("append").save(TABLE_PATH)


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \


spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.conf.set("spark.databricks.delta.write.isolationLevel",
               "WriteSerializable")

# delete the current delta table
os.system(f"rm -rf {TABLE_PATH}")

# create a new delta TABLE with column id
df = spark.createDataFrame([(0,)], ["id"])
# write the table in serialized isolation level
df.write.format("delta").save(TABLE_PATH)

df_reader = spark.read.format("delta").load(TABLE_PATH)
df_reader.show()


# Do three concurrent writes to the table and three concurrent reads

threads_list = []
for i in range(3):
    # create write thread
    df1 = spark.createDataFrame([(i+1,)], ["id"])
    threads_list.append(threading.Thread(target=append_to_table, args=(df1,)))
    # create read thread
    threads_list.append(threading.Thread(target=read_table))


for thread in threads_list:
    thread.start()

for thread in threads_list:
    thread.join()

df_reader = spark.read.format("delta").load(TABLE_PATH)
df_reader.show()
