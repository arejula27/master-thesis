from pyspark.sql.types import StructType, StringType
import statistics
import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from glob import glob
from pyspark.sql.functions import col, from_json
import time
import re

# Paths
INPUT_DIR = "data/github_by_type/"
TABLE_PATH = "delta-table-json-serialization-values"
STRING_TABLE_PATH = "delta-table-json-serialization-strings"
NUM_RUNS = 1

files = glob(os.path.join(INPUT_DIR, "*.json"))

# ====== Helper Functions ======


def process_file_store_values(spark, file_paths):
    try:
        df = spark.read.json(file_paths)
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(TABLE_PATH)
        print(f"✅ Stored JSON values from {
              len(file_paths)} files in Delta table")
    except (AnalysisException, Exception) as e:
        print(f"❌ Error processing values: {e}")


def process_file_store_string(spark, file_paths):
    try:
        raw_df = spark.read.text(file_paths)
        json_df = raw_df.withColumnRenamed("value", "json_event")
        json_df.write.format("delta") \
            .mode("append") \
            .save(STRING_TABLE_PATH)
        print(f"✅ Stored JSON strings from {
              len(file_paths)} files in Delta table")
    except (AnalysisException, Exception) as e:
        print(f"❌ Error processing strings: {e}")


# ====== Start Spark ======
spark = SparkSession.builder \
    .appName("JsonSerializationExperiment") \
    .getOrCreate()

master = spark.sparkContext.getConf().get("spark.master")

# Extract thread count from master URL if local[N]
match = re.match(r"local\[(\d+|\*)\]", master)
if match:
    num_threads = match.group(1)
else:
    num_threads = "unknown"

print(f"\n=== Running Spark with master={master} | Threads: {num_threads} ===")

# ====== Prepare Output ======
csv_path = "json_serialization_times.csv"
if not os.path.exists(csv_path):
    with open(csv_path, "w") as f:
        f.write("num_threads,write_values_time,"
                "write_strings_time,read_values_time,read_strings_time\n")

# ====== Benchmark Loop ======
value_write_times = []
string_write_times = []
value_read_times = []
string_read_times = []

for run in range(NUM_RUNS):
    print(f"\n▶️ Run {run + 1}/{NUM_RUNS}")

    os.system(f"rm -rf {TABLE_PATH}")
    os.system(f"rm -rf {STRING_TABLE_PATH}")

    # Write JSON values
    start = time.time()
    process_file_store_values(spark, files)
    value_write_times.append(time.time() - start)

    # Write JSON strings
    start = time.time()
    process_file_store_string(spark, files)
    string_write_times.append(time.time() - start)

    # Read values and count distinct types
    start = time.time()
    df_values = spark.read.format("delta").load(TABLE_PATH)
    _ = df_values.select("type").distinct().count()
    value_read_times.append(time.time() - start)

    # Read strings, parse JSON, count distinct types
    start = time.time()
    df_raw = spark.read.format("delta").load(STRING_TABLE_PATH)
    json_schema = StructType().add("type", StringType())
    df_parsed = df_raw.withColumn(
        "parsed", from_json(col("json_event"), json_schema))
    _ = df_parsed.select(col("parsed.type")).distinct().count()
    string_read_times.append(time.time() - start)

# ====== Final Stats ======
spark.stop()

wv_mean = statistics.mean(value_write_times)
# wv_std = statistics.stdev(value_write_times)
ws_mean = statistics.mean(string_write_times)
# ws_std = statistics.stdev(string_write_times)
rv_mean = statistics.mean(value_read_times)
# rv_std = statistics.stdev(value_read_times)
rs_mean = statistics.mean(string_read_times)
# rs_std = statistics.stdev(string_read_times)

print(f"\n📊 Results for {num_threads} thread(s):")
print(f"  Write Values: {wv_mean:.2f} sec")
print(f"  Write Strings: {ws_mean:.2f} sec")
print(f"  Read Values: {rv_mean:.2f} sec")
print(f"  Read Strings: {rs_mean:.2f} sec")

# Append to CSV
with open(csv_path, "a") as f:
    f.write(f"{num_threads},{wv_mean:.2f},"
            f"{ws_mean:.2f},{rv_mean:.2f},{rs_mean:.2f}\n")
