import os
import threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

TABLE_PATH = "delta-table-nested-json"

# delete the table if it exists


os.system(f"rm -rf {TABLE_PATH}")
spark = SparkSession.builder \
    .appName("CreateTable") \
    .getOrCreate()

# Path to input JSON and output Delta
input_path = "data/nested.json"
input_path_2 = "data/nested_2.json"
input_path_3 = "data/nested_3.json"

# Read the JSON file (automatically infers schema including nested structure)
df = spark.read.json(input_path)

# (Optional) Print schema to confirm nested structure
df.printSchema()

# Write the DataFrame to Delta format
df.write.format("delta").mode("overwrite").save(TABLE_PATH)
df = spark.read.format("delta").load(TABLE_PATH)
df.show()


# Modify "name" from Alice -> Elisa
df = df.withColumn("name", when(col("name") == "Alice",
                   "Elisa").otherwise(col("name")))

# Overwrite the table with modified data
df.write.format("delta").mode("overwrite").save(TABLE_PATH)

# Read back and verify change
df = spark.read.format("delta").load(TABLE_PATH)
df.show(truncate=False)


# Write concurrently both files to the same Delta TABLE_PATH

df1 = spark.read.option("multiline", "true").json(input_path_3)
df2 = spark.read.option("multiline", "true").json(input_path_2)
thread1 = threading.Thread(target=df1.write.format(
    "delta").mode("append").save, args=(TABLE_PATH,))
thread2 = threading.Thread(target=df2.write.format(
    "delta").mode("append").save, args=(TABLE_PATH,))
thread1.start()
thread2.start()


thread1.join()
thread2.join()
# Read back and verify changes
df_reader = spark.read.format("delta").load(TABLE_PATH)
df_reader.show(truncate=False)
