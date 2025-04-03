import os
import time
from tqdm import tqdm
import io
import sys
import random
import string
import pyspark
from delta import *
from collections import defaultdict
from delta.tables import *
import threading
from functools import wraps
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Tuple

TABLE_PATH = "delta-table-bench"

# Number of seconds to run the test
SECONDS = 5
# Concurrent readers and writers per second
NUM_READERS = 2
NUM_WRITERS = 1
# Max concurrent threads
MAX_THREADS = NUM_READERS + NUM_WRITERS


# Create a queue to hold the read and write operations results
# It is concurrently safe for multiple threads


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


""""
def run_operations(operations, max_time=1):
    threads = []

    for func, num_threads in operations:
        start_time = time.time()
        for _ in range(num_threads):
            thread = threading.Thread(target=func)
            threads.append(thread)
            # Start the thread
            thread.start()

        for thread in threads:
            thread.join(timeout=1)

        elapsed_time = time.time() - start_time

        time_to_sleep = max(0, max_time - elapsed_time)
        time.sleep(time_to_sleep)  # Sleep for the remaining time


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

operations = [(read_delta_table, NUM_READERS),
              (append_delta_table, NUM_WRITERS)]

original_stdout = sys.stdout
sys.stdout = io.StringIO()
for _ in range(SECONDS):
    # capture the stdout
    # Start all threads
    run_operations(operations)
    # Restore the stdout
sys.stdout = original_stdout
"""


#######

def worker(tasks: queue.Queue[Callable[[], None]],
           results: queue.Queue[Tuple[str, bool]]):
    while True:
        # Get the next task from the queue
        operation = tasks.get(block=True, timeout=2)
        try:
            operation()
            results.put((operation.__name__, True), block=False)
        except Exception:
            results.put((operation.__name__, False), block=False)
        finally:
            # Mark the task as done
            tasks.task_done()


def dummy_func():
    # Simulate some work
    time.sleep(1)
    # print the thread Name
    print(f"Thread {threading.current_thread().name} is working")


def print_stats(operation_count):
    print("=== Threads finished ===")
    print(f"Total number of threads: {MAX_THREADS}")

    total_success = sum(counts['success']
                        for counts in operation_count.values())
    total_failure = sum(counts['failure']
                        for counts in operation_count.values())
    print(f"Total number of operations: {total_success + total_failure}")
    print(f"Total number of successful operations: {total_success}")
    print(f"Total number of failed operations: {total_failure}")
    print("=== Operation Results ===")
    for operation, counts in operation_count.items():
        print(f"Operation: {operation}")
        print(f"\tSuccessful operations: {counts['success']}")
        print(f"\tFailed operations: {counts['failure']}")


def main():

    tasks = queue.Queue()
    results = queue.Queue()

    operations = [(read_delta_table, NUM_READERS),
                  (append_delta_table, NUM_WRITERS)]
    operation_count = defaultdict(lambda: {"success": 0, "failure": 0})
    # Create a thread pool executor
    executor = ThreadPoolExecutor(max_workers=MAX_THREADS)
    # Start all threads
    for _ in range(MAX_THREADS):
        executor.submit(worker, tasks, results)

    for _ in range(SECONDS):
        tasks.put(dummy_func, block=False)
        tasks.put(dummy_func, block=False)
        tasks.put(dummy_func, block=False)
        tasks.put(dummy_func, block=False)
        time.sleep(1)

    # Wait for all tasks to be done
    tasks.join()
    print("=== All tasks done ===")

    # Count the number of successful and failed operations
    operation_count = defaultdict(lambda: {"success": 0, "failure": 0})
    while not results.empty():
        operation, success = results.get()
        results.task_done()
        # Actualizar los contadores en el diccionario
        if success:
            operation_count[operation]["success"] += 1
        else:
            operation_count[operation]["failure"] += 1

    # Print the results
    print_stats(operation_count)

    # Shutdown the executor
    executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
