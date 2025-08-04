import os
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from glob import glob


# Paths
INPUT_DIR = "data/github_by_type/"
TABLE_PATH = "delta-table-hourly-json"

os.system(f"rm -rf {TABLE_PATH}")
spark = SparkSession.builder \
    .appName("CreateTable") \
    .getOrCreate()

# Function to process one file


def process_file(file_path):
    try:
        df = spark.read.json(file_path)
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(TABLE_PATH)
        print(f"✅ Processed {file_path}")
    except AnalysisException as e:
        print(f"⚠️ Failed to process {file_path}: {e}")
    except Exception as e:
        print(f"❌ Unexpected error on {file_path}: {e}")


# Use ThreadPoolExecutor to run in parallel
# Adjust number of workers as needed
files = glob(os.path.join(INPUT_DIR, "*.json"))
process_file(files[0])  # Process the first file to create the table schema
with ThreadPoolExecutor(max_workers=4) as executor:
    # Skip the first file since it's already processed
    executor.map(process_file, files[1:])

# Show results (optional)
df = spark.read.format("delta").load(TABLE_PATH)
# print the number of records
print(f"Total records in Delta table: {df.count()}")
# show the schema
