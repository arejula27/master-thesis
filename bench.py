import os
import io
import sys
import random
import string
import pyspark
from delta import *
from delta.tables import *
import threading
from functools import wraps
import queue


TABLE_PATH = "delta-table-bench"

# Concurrent readers and writers per second
NUM_READERS = 10
NUM_WRITERS = 3


# Create a queue to hold the read and write operations results
# It is concurrently safe for multiple threads
queue = queue.Queue()


def monitor(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)  # Call the function
            # if the function is successful update the queue
            queue.put((func.__name__, True))
        except Exception:
            # if the function fails update the queue
            queue.put((func.__name__, False))

    return wrapper


def random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


@monitor
def read_delta_table():
    # Read the delta table
    df = spark.read.format("delta").load(TABLE_PATH)
    df.show()


@monitor
def append_delta_table():
    # Append a new row to the delta table
    data = [(random_string(),)]
    columns = ["col1"]
    df = spark.createDataFrame(data, columns)
    df.write.format("delta").mode("append").save(TABLE_PATH)


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

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

# create a pool of threads
threads = []
for i in range(NUM_READERS):
    thread = threading.Thread(target=read_delta_table)
    threads.append(thread)
for i in range(NUM_WRITERS):
    thread = threading.Thread(target=append_delta_table)
    threads.append(thread)


# Redirect stdout to capture print statements

original_stdout = sys.stdout
sys.stdout = io.StringIO()
# Start all threads
for thread in threads:
    thread.start()
# Wait for all threads to finish
for thread in threads:
    thread.join()
sys.stdout = original_stdout

# Count the number of successful and failed operations
success_read_count = 0
failure_read_count = 0
success_write_count = 0
failure_write_count = 0

while not queue.empty():
    # switch to determine the operation type
    operation, success = queue.get()
    print(f"Operation: {operation}, Success: {success}")
    match operation:
        case "read_delta_table":
            if success:
                success_read_count += 1
            else:
                failure_read_count += 1
        case "append_delta_table":
            if success:
                success_write_count += 1
            else:
                failure_write_count += 1

print("=== Threads finished ===")
print(f"Number of threads: {len(threads)}")
print(f"Number of readers: {NUM_READERS}")
print(f"Number of writers: {NUM_WRITERS}")
print(f"Total number of successful operations: {
      success_read_count + success_write_count}")
print(f"Total number of failed operations: {
      failure_read_count + failure_write_count}")
print(f"\tSuccessful read operations: {success_read_count}")
print(f"\tFailed read operations: {failure_read_count}")
print(f"\tSuccessful write operations: {success_write_count}")
print(f"\tFailed write operations: {failure_write_count}")
