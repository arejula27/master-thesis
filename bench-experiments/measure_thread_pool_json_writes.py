import os
import time
import statistics
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
from glob import glob
from pyspark.sql import SparkSession

MAX_THREADS = 10
INPUT_DIR = "data/github_by_type/"
TABLE_PATH = "delta-table-json-serialization-values-pool"
STRING_TABLE_PATH = "delta-table-json-serialization-strings-pool"

OUTPUT_CSV_PATH = "json_serialization_pool_times.csv"
files = glob(os.path.join(INPUT_DIR, "*.json"))


# ====== Start Spark ======
spark = SparkSession.builder \
    .appName("JsonSerializationExperiment") \
    .getOrCreate()

# ====== Helper Functions ======


def process_file_store_values(file_path):
    try:
        df = spark.read.json(file_path)
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(TABLE_PATH)
        print(f"✅ Stored JSON values from {
              file_path} in Delta table")
    except (AnalysisException, Exception) as e:
        print(f"❌ Error processing values: {e}")


def process_file_store_string(file_path):
    try:
        raw_df = spark.read.text(file_path)
        json_df = raw_df.withColumnRenamed("value", "json_event")
        json_df.write.format("delta") \
            .mode("append") \
            .save(STRING_TABLE_PATH)
        print(f"✅ Stored JSON strings from {
              file_path}  in Delta table")
    except (AnalysisException, Exception) as e:
        print(f"❌ Error processing strings: {e}")

        # Setup output file with headers
with open(OUTPUT_CSV_PATH, "w") as f:
    f.write("num_threads,write_values_mean,write_values_stdev,"
            "write_strings_mean,write_strings_stdev\n")


for num_thread in range(1, MAX_THREADS):
    value_write_times = []
    string_write_times = []
    value_read_times = []
    string_read_times = []

    for _ in range(10):  # Run 10 times for each thread count
        # Clean up
        os.system(f"rm -rf {TABLE_PATH}")
        os.system(f"rm -rf {STRING_TABLE_PATH}")

        print(f"\n🔁 Running trial with {num_thread} thread(s)...")

        # Write JSON values
        start = time.time()
        process_file_store_values(files[0])
        with ThreadPoolExecutor(max_workers=num_thread) as executor:
            executor.map(process_file_store_values, files[1:])
        value_write_times.append(time.time() - start)

        # Write JSON strings
        start = time.time()
        process_file_store_string(files[0])
        with ThreadPoolExecutor(max_workers=num_thread) as executor:
            executor.map(process_file_store_string, files[1:])
        string_write_times.append(time.time() - start)

    # Compute statistics
    wv_mean = statistics.mean(value_write_times)
    wv_std = statistics.stdev(value_write_times)

    ws_mean = statistics.mean(string_write_times)
    ws_std = statistics.stdev(string_write_times)

    print(f"\nResults for {num_thread} thread(s):")
    print(f"Write Values: Mean = {wv_mean:.2f}s, Std Dev = {wv_std:.2f}s")
    print(f"Write Strings: Mean = {ws_mean:.2f}s, Std Dev = {ws_std:.2f}s")

    # Write the mean and stdev to CSV
    with open(OUTPUT_CSV_PATH, "a") as f:
        f.write(f"{num_thread},{wv_mean:.2f},{wv_std:.2f},"
                f"{ws_mean:.2f},{ws_std:.2f}\n")
