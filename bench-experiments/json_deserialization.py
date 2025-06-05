from pyspark.sql.types import StructType, StringType
import statistics
import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from glob import glob
from pyspark.sql.functions import col, from_json
import time

# Paths
INPUT_DIR = "data/github_by_type/"
TABLE_PATH = "delta-table-json-serialization-values"
STRING_TABLE_PATH = "delta-table-json-serialization-strings"

MAX_THREADS = 10
NUM_RUNS = 10

files = glob(os.path.join(INPUT_DIR, "*.json"))

# Function to process one file (store as parsed JSON values)


def process_file_store_values(spark, file_paths):
    try:
        # Read all JSON files at once (Spark supports reading multiple files via wildcard or list)
        df = spark.read.json(file_paths)
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(TABLE_PATH)
        print(f"✅ Stored JSON values from {
              len(file_paths)} files in Delta table")
    except AnalysisException as e:
        print(f"⚠️ Failed to process files: {e}")
    except Exception as e:
        print(f"❌ Unexpected error on processing files: {e}")

# Function to process one file (store as raw JSON strings)


def process_file_store_string(spark, file_paths):
    try:
        # Read all files as text lines, each line is a JSON string
        # single DataFrame with column 'value'
        raw_df = spark.read.text(file_paths)
        json_df = raw_df.withColumnRenamed("value", "json_event")

        json_df.write.format("delta") \
            .mode("append") \
            .save(STRING_TABLE_PATH)
        print(f"✅ Stored JSON strings from {
              len(file_paths)} files in Delta table")
    except AnalysisException as e:
        print(f"⚠️ Failed to process files: {e}")
    except Exception as e:
        print(f"❌ Unexpected error on processing files: {e}")


# Prepare CSV file for results
# if os.path.exists("json_serialization_times.csv"):
#    os.remove("json_serialization_times.csv")
if not os.path.exists("json_serialization_times.csv"):
    # Write header if file does not exist
    with open("json_serialization_times.csv", "w") as f:
        f.write("num_threads,write_values_mean,write_values_stdev,"
                "write_strings_mean,write_strings_stdev,"
                "read_values_mean,read_values_stdev,"
                "read_strings_mean,read_strings_stdev\n")

for num_thread in range(4, MAX_THREADS):
    value_write_times = []
    string_write_times = []
    value_read_times = []
    string_read_times = []

    print(f"\n=== Running with Spark using {num_thread} thread(s) ===")

    for run in range(NUM_RUNS):
        # Clean up old tables
        os.system(f"rm -rf {TABLE_PATH}")
        os.system(f"rm -rf {STRING_TABLE_PATH}")

        # Create Spark session with local[num_thread]
        spark = SparkSession.builder \
            .appName("JsonSerializationExperiment") \
            .master(f"local[{num_thread}]") \
            .config("spark.sql.shuffle.partitions", num_thread) \
            .getOrCreate()

        print(f"Run {run+1}/{NUM_RUNS}")

        # Write JSON values (sequentially for all files)
        start = time.time()
        process_file_store_values(spark, files)
        value_write_times.append(time.time() - start)

        # Write JSON strings (sequentially for all files)
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

        spark.stop()

    # Compute mean and stdev
    wv_mean = statistics.mean(value_write_times)
    wv_std = statistics.stdev(value_write_times) if len(
        value_write_times) > 1 else 0

    ws_mean = statistics.mean(string_write_times)
    ws_std = statistics.stdev(string_write_times) if len(
        string_write_times) > 1 else 0

    rv_mean = statistics.mean(value_read_times)
    rv_std = statistics.stdev(value_read_times) if len(
        value_read_times) > 1 else 0

    rs_mean = statistics.mean(string_read_times)
    rs_std = statistics.stdev(string_read_times) if len(
        string_read_times) > 1 else 0

    print(f"Results for {num_thread} thread(s):")
    print(f"  Write Values: {wv_mean:.2f} ± {wv_std:.2f} seconds")
    print(f"  Write Strings: {ws_mean:.2f} ± {ws_std:.2f} seconds")
    print(f"  Read Values: {rv_mean:.2f} ± {rv_std:.2f} seconds")
    print(f"  Read Strings: {rs_mean:.2f} ± {rs_std:.2f} seconds")
    print("=" * 40)
    # Write results to CSV
    with open("json_serialization_times.csv", "a") as f:
        f.write(f"{num_thread},{wv_mean:.2f},{wv_std:.2f},"
                f"{ws_mean:.2f},{ws_std:.2f},"
                f"{rv_mean:.2f},{rv_std:.2f},"
                f"{rs_mean:.2f},{rs_std:.2f}\n")
