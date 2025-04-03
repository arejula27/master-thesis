import os
import time
import io
import sys
import random
import string
import pyspark
from delta import *
from collections import defaultdict
from delta.tables import *
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Tuple

TABLE_PATH = "delta-table-bench"

# Number of seconds to run the test
ITERATIONS = 10
# Concurrent readers and writers per second
NUM_READERS = 5
NUM_WRITERS = 3
NUM_WRITER_SCHEMA_CHANGE = 1
# Max concurrent threads
MAX_THREADS = NUM_READERS + NUM_WRITERS + NUM_WRITER_SCHEMA_CHANGE

# Set to True will retry the failed operations after a delay
RETTRY_FAILED = False


def random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def read_delta_table():
    # Read the delta table
    df = spark.read.format("delta").load(TABLE_PATH)
    df.show()


def append_delta_row():
    # Append a new row to the delta table
    data = [(random_string(),)]
    columns = ["col1"]
    df = spark.createDataFrame(data, columns)
    df.write.format("delta").mode("append").save(TABLE_PATH)


def change_schema_and_append_delta_row():
    # Append a new row to the delta table
    data = [(random_string(), random_string())]
    columns = ["col1", random_string()]
    df = spark.createDataFrame(data, columns)
    df.write.format("delta").mode("append").save(TABLE_PATH)


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Enable auto schema merging, this will allow us to merge the schema automatically in all delta lake tables
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# delete the current delta table
os.system(f"rm -rf {TABLE_PATH}")

# create a new delta TABLE
data = [(random_string(),)]
columns = ["col1"]
df = spark.createDataFrame(data, columns)
df.write.format("delta").save(TABLE_PATH)


def worker(tasks: queue.Queue[Callable[[], None]],
           results: queue.Queue[Tuple[str, bool]]):
    while True:
        # Get the next task from the queue
        operation, attempt = tasks.get(block=True, timeout=2)
        # sleep for attemps *0.5 seconds
        time.sleep(attempt * 0.5)
        start_time = time.time()
        try:
            operation()
            end_time = time.time()
            results.put((operation.__name__, True,
                        end_time-start_time), block=False)
        except Exception as e:
            print(f"Error: {e}")
            end_time = time.time()
            results.put((operation.__name__, False,
                        end_time-start_time), block=False)
            if RETTRY_FAILED:
                # Retry the operation
                tasks.put((operation, attempt+1), block=False)

        finally:
            # Mark the task as done
            tasks.task_done()


def print_stats(operation_task_per_iter, operation_count, global_time_taken, all_operations_time):
    print("=== Statistics ===")
    print(f"Total number of threads: {MAX_THREADS}")

    total_success = sum(counts['success']
                        for counts in operation_count.values())
    total_failure = sum(counts['failure']
                        for counts in operation_count.values())
    print(f"Total number of operations: {total_success + total_failure}")
    print(f"Total number of iterations: {ITERATIONS}")
    for operation, number in operation_task_per_iter:
        print(f"\tNumber of {
              operation.__name__} per second called: {number}")

    print("=== Operation Results ===")
    print(f"Total time taken: {global_time_taken:.2f} seconds")
    # calc average time taken
    average_time_taken = sum(all_operations_time) / len(all_operations_time)
    print(f"Average time taken per operation: {
          average_time_taken:.2f} seconds")
    print(f"Total number of successful operations: {total_success}")

    print(f"Total number of failed operations: {total_failure}")

    for operation, counts in operation_count.items():
        print(f"Operation: {operation}")
        print(f"\tSuccessful operations: {counts['success']}")
        print(f"\tFailed operations: {counts['failure']}")

    print("All operations time taken:")
    print(f"{all_operations_time}")


def main():

    tasks = queue.Queue()
    results = queue.Queue()

    operations = [(read_delta_table, NUM_READERS),
                  (append_delta_row, NUM_WRITERS),
                  (change_schema_and_append_delta_row, NUM_WRITER_SCHEMA_CHANGE),
                  ]
    operation_count = defaultdict(lambda: {"success": 0, "failure": 0})
    # Create a thread pool executor
    executor = ThreadPoolExecutor(max_workers=MAX_THREADS)
    # Start all threads
    for _ in range(MAX_THREADS):
        executor.submit(worker, tasks, results)

    # capture the stdout and stderr
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    start_time = time.time()
    for _ in range(ITERATIONS):
        for operation, count in operations:
            for _ in range(count):
                # Add the operation to the queue
                # format (operation, attempt)
                # The attempt is always 0 for the first time
                tasks.put((operation, 0), block=False)
        time.sleep(1)

    # Wait for all tasks to be done
    tasks.join()
    end_time = time.time()
    # Restore stdout and stderr
    sys.stdout = old_stdout
    sys.stderr = old_stderr

    # Count the number of successful and failed operations
    operation_count = defaultdict(lambda: {"success": 0, "failure": 0})
    times_taken = []
    while not results.empty():
        operation, success, time_taken = results.get()
        results.task_done()
        times_taken.append(time_taken)
        # Actualizar los contadores en el diccionario
        if success:
            operation_count[operation]["success"] += 1
        else:
            operation_count[operation]["failure"] += 1

    # Print the results
    print_stats(operations, operation_count, end_time-start_time, times_taken)

    # Shutdown the executor
    executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
