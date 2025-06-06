import os
import argparse
import time
import random
import string
from pyspark.sql import SparkSession
from collections import defaultdict
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Tuple
import csv


TABLE_PATH = "delta-table-bench"

# Number of seconds to run the test
ITERATIONS = 10
# Concurrent readers and writers per second
NUM_READERS = 3
NUM_WRITERS = 3
NUM_WRITER_SCHEMA_CHANGE = 0
# Max concurrent threads
MAX_THREADS = NUM_READERS + NUM_WRITERS + NUM_WRITER_SCHEMA_CHANGE

# Set to True will retry the failed operations after a delay
RETTRY_FAILED = False

# Default name of the experiment
EXPERIMENT_DEFAULT_NAME = "benchmark"
SAVE_STATS = False

SLEEP_TIME = 5


def random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def read_delta_table():
    # Read the delta table
    df = spark.read.format("delta").load(TABLE_PATH)
    # df.show()


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


spark = SparkSession.builder \
    .appName("bench") \
    .getOrCreate()

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
        # Get the next task from the queue, the timeout will
        operation, attempt = tasks.get(block=True, timeout=SLEEP_TIME+1)
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


def calculate_time_averages(operation_count):
    """
    Calculates average times for success, failure, and overall operations.

    Args:
        operation_count (dict): A dictionary where keys are operation names and values are dictionaries
                                with "success" and "failure" entries. Each entry is a tuple:
                                (count, list of time durations).

    Returns:
        dict: A dictionary containing:
            - average_success_time_per_operation: Average success time per operation.
            - average_failure_time_per_operation: Average failure time per operation.
            - average_time_per_operation: Average time (success + failure) per operation.
            - average_time: Global average time across all operations and outcomes.
    """
    average_success_time_per_operation = {}
    average_failure_time_per_operation = {}
    average_time_per_operation = {}
    total_times = []

    for operation, counts in operation_count.items():
        success_count, success_times = counts["success"]
        failure_count, failure_times = counts["failure"]
        print(f"Operation: {operation}")
        print(f"\tSuccessful times: {success_times}")

        # Calculate average success time
        if success_count > 0:
            avg_success = sum(success_times) / success_count
        else:
            avg_success = 0
        average_success_time_per_operation[operation] = avg_success

        # Calculate average failure time
        if failure_count > 0:
            avg_failure = sum(failure_times) / failure_count
        else:
            avg_failure = 0
        average_failure_time_per_operation[operation] = avg_failure

        # Calculate average time per operation
        total_operation_times = success_times + failure_times
        total_count = success_count + failure_count
        if total_count > 0:
            avg_total = sum(total_operation_times) / total_count
            total_times.extend(total_operation_times)
        else:
            avg_total = 0
        average_time_per_operation[operation] = avg_total

    # Calculate global average time across all operations
    if total_times:
        average_time = sum(total_times) / len(total_times)
    else:
        average_time = 0

    return {
        "average_success_time_per_operation": average_success_time_per_operation,
        "average_failure_time_per_operation": average_failure_time_per_operation,
        "average_time_per_operation": average_time_per_operation,
        "average_time": average_time
    }


def calculate_stats(operation_task_per_iter, operation_count, global_time_taken):
    total_success = sum(counts['success'][0]
                        for counts in operation_count.values())
    total_failure = sum(counts['failure'][0]
                        for counts in operation_count.values())
    operations_time = calculate_time_averages(operation_count)

    # make a default dict to store operation_details
    operation_details = defaultdict(lambda: {
                                    "average_time": 0, "success_count": 0,
                                    "failure_count": 0,
                                    "average_success_time": 0,
                                    "average_failure_time": 0})
    stats = {
        "total_time_taken": global_time_taken,
        "total_success": total_success,
        "total_failure": total_failure,
        "average_time": operations_time["average_time"],
        "operation_details": operation_details
    }

    for operation_name, counts in operation_count.items():
        stats["operation_details"][operation_name] = {
            "average_time": operations_time["average_time_per_operation"].get(operation_name, 0),
            "success_count": counts['success'][0],
            "failure_count": counts['failure'][0],
            "average_success_time": operations_time["average_success_time_per_operation"].get(operation_name, 0),
            "average_failure_time": operations_time["average_failure_time_per_operation"].get(operation_name, 0),
        }
    return stats


def print_stats(stats):
    print("=== Statistics ===")
    print(f"Total time taken: {stats['total_time_taken']:.2f} seconds")
    print(f"Total number of successful operations: {stats['total_success']}")
    print(f"Total number of failed operations: {stats['total_failure']}")
    print(f"Average time per operation: {stats['average_time']:.2f} seconds")

    print("\n=== Operation Details ===")
    for operation_name, details in stats['operation_details'].items():
        print(f"Operation: {operation_name}")
        print(f"\tAverage time: {details['average_time']:.2f} seconds")
        print(f"\tSuccessful operations: {details['success_count']}")
        print(f"\t\tAverage time on success: {
              details['average_success_time']:.2f} seconds")
        print(f"\tFailed operations: {details['failure_count']}")


def log_stats(stats, operation_names, config):
    file_name = f"{EXPERIMENT_DEFAULT_NAME}.csv"
    file_exists = os.path.exists(file_name)
    with open(file_name, mode="a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            # Write header if file doesn't exist
            header = ["total_time_taken", "total_success",
                      "total_failure", "average_time"]
            for operation_name in sorted(operation_names):
                header.extend([
                    f"average_time_{operation_name}",
                    f"success_count_{operation_name}",
                    f"failure_count_{operation_name}",
                    f"average_success_time_{operation_name}",
                    f"average_failure_time_{operation_name}"
                ])
            header.extend(sorted(config.keys()))
            writer.writerow(header)
        # Write the row with current stats
        row = [
            stats['total_time_taken'],
            stats['total_success'],
            stats['total_failure'],
            stats['average_time']
        ]
        for operation_name in sorted(operation_names):
            details = stats["operation_details"][operation_name]
            row.extend([
                details['average_time'],
                details['success_count'],
                details['failure_count'],
                details['average_success_time'],
                details['average_failure_time']
            ])
        # Add the config values to the row
        for key in sorted(config.keys()):
            row.append(config[key])

        writer.writerow(row)


def parse_flags():
    global NUM_READERS, NUM_WRITERS, NUM_WRITER_SCHEMA_CHANGE, MAX_THREADS, RETTRY_FAILED, ITERATIONS, SAVE_STATS, EXPERIMENT_DEFAULT_NAME
    parser = argparse.ArgumentParser(
        description="Configure concurrent readers and writers.")
    parser.add_argument('--iterations', type=int,
                        default=ITERATIONS, help="Number of iterations to run the test, the number of readers and writters will be N times the iterations.")
    parser.add_argument('--num-readers', type=int,
                        default=NUM_READERS, help="Number of concurrent readers.")
    parser.add_argument('--num-writers', type=int,
                        default=NUM_WRITERS, help="Number of concurrent writers.")
    parser.add_argument('--num-writer-schema-change', type=int, default=NUM_WRITER_SCHEMA_CHANGE,
                        help="Number of concurrent schema-changing writers, this will create conflicts.")
    parser.add_argument('--retry-failed', action='store_true',
                        default=RETTRY_FAILED, help="Retry failed operations, when a task fails it will be readded to the tasks queue.")

    parser.add_argument('--max-threads', type=int,
                        help="Max number of concurrent threads.")
    parser.add_argument('--name', type=str, default=EXPERIMENT_DEFAULT_NAME,
                        help="Name of the experiment, this will be used to create the log file.")
    parser.add_argument('--save', default=False, action='store_true',
                        help="Save the stats to a file. Set the name of the experiment with --name flag.")
    args = parser.parse_args()

    # Set the global variables based on flags

    ITERATIONS = args.iterations
    NUM_READERS = args.num_readers
    NUM_WRITERS = args.num_writers
    NUM_WRITER_SCHEMA_CHANGE = args.num_writer_schema_change
    RETTRY_FAILED = args.retry_failed
    MAX_THREADS = args.max_threads if args.max_threads is not None else (
        NUM_READERS + NUM_WRITERS + NUM_WRITER_SCHEMA_CHANGE)
    EXPERIMENT_DEFAULT_NAME = args.name
    SAVE_STATS = args.save

    # Print the configuration
    print("=== Configuration ===")
    print(f"Number of readers: {NUM_READERS}")
    print(f"Number of writers: {NUM_WRITERS}")
    print(f"Number of writers with schema change: {NUM_WRITER_SCHEMA_CHANGE}")
    print(f"Number of iterations: {ITERATIONS}")
    print(f"Max number of threads: {MAX_THREADS}")
    print(f"Retry failed operations: {RETTRY_FAILED}")

    config = {
        "num_readers": NUM_READERS,
        "num_writers": NUM_WRITERS,
        "num_writer_schema_change": NUM_WRITER_SCHEMA_CHANGE,
        "num_iterations": ITERATIONS,
        "max_threads": MAX_THREADS,
        "retry_failed": RETTRY_FAILED,
        "experiment_name": EXPERIMENT_DEFAULT_NAME,
        "save_stats": SAVE_STATS
    }
    return config


def main():

    # print arguments
    conf = parse_flags()
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
    # old_stdout = sys.stdout
    # old_stderr = sys.stderr
    # sys.stdout = io.StringIO()
    # sys.stderr = io.StringIO()
    print("=== Starting the test ===")
    start_time = time.time()
    for _ in range(ITERATIONS):
        print(f"Iteration {_+1} of {ITERATIONS}")
        for operation, count in operations:
            for _ in range(count):
                # Add the operation to the queue
                # format (operation, attempt)
                # The attempt is always 0 for the first time
                tasks.put((operation, 0), block=False)
        time.sleep(SLEEP_TIME)

    # Wait for all tasks to be done
    print("Waiting for all tasks to be done...")
    tasks.join()
    print("All tasks done.")
    end_time = time.time()
    # Restore stdout and stderr
    # sys.stdout = old_stdout
    # sys.stderr = old_stderr

    # Count the number of successful and failed operations
    #
    operation_count = defaultdict(
        lambda: {"success": [0, []], "failure": [0, []]})
    while not results.empty():
        operation, success, time_taken = results.get()
        results.task_done()
        # This print is for debugging purposes, it will print the order of the txs
        # print(operation, success)
        # Actualizar los contadores en el diccionario
        if success:
            operation_count[operation]["success"][0] += 1
            operation_count[operation]["success"][1].append(time_taken)
        else:
            operation_count[operation]["failure"][0] += 1
            operation_count[operation]["failure"][1].append(time_taken)

    # Print the results
    stats = calculate_stats(operations, operation_count,
                            end_time-start_time)

    print_stats(stats)
    if SAVE_STATS:
        operation_names = [op[0].__name__ for op in operations]
        log_stats(stats, operation_names, conf)
        print(f"Stats saved to {EXPERIMENT_DEFAULT_NAME}.csv")
    else:
        print("Stats not saved, use --save flag to save the stats.")
    # Shutdown the executor
    executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
