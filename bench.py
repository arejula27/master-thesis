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
import statistics


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
        "average_time": global_time_taken,
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


def log_summary_stats(summary_stats, config):
    file_name = f"{EXPERIMENT_DEFAULT_NAME}.csv"
    file_exists = os.path.exists(file_name)

    with open(file_name, mode="a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            header = [
                "average_time_mean",
                "average_time_std",
                "total_success_mean",
                "total_success_std",
                "total_failure_mean",
                "total_failure_std"
            ]
            for op_name in sorted(summary_stats["operation_summaries"].keys()):
                header.extend([
                    f"{op_name}_mean_time",
                    f"{op_name}_std_time",
                    f"{op_name}_mean_success",
                    f"{op_name}_std_success",
                    f"{op_name}_mean_failure",
                    f"{op_name}_std_failure"
                ])
            header.extend(sorted(config.keys()))
            writer.writerow(header)

        row = [
            summary_stats["average_time_mean"],
            summary_stats["average_time_std"],
            summary_stats["total_success_mean"],
            summary_stats["total_success_std"],
            summary_stats["total_failure_mean"],
            summary_stats["total_failure_std"]
        ]
        for op_name in sorted(summary_stats["operation_summaries"].keys()):
            op_stats = summary_stats["operation_summaries"][op_name]
            row.extend([
                op_stats["mean_time"],
                op_stats["std_time"],
                op_stats["mean_success"],
                op_stats["std_success"],
                op_stats["mean_failure"],
                op_stats["std_failure"]
            ])
        for key in sorted(config.keys()):
            row.append(config[key])
        writer.writerow(row)


def main():
    conf = parse_flags()
    tasks = queue.Queue()
    results = queue.Queue()

    operations = [(read_delta_table, NUM_READERS),
                  (append_delta_row, NUM_WRITERS),
                  (change_schema_and_append_delta_row, NUM_WRITER_SCHEMA_CHANGE)]

    executor = ThreadPoolExecutor(max_workers=MAX_THREADS)
    for _ in range(MAX_THREADS):
        executor.submit(worker, tasks, results)

    print("=== Starting the test ===")
    iteration_stats = []  # Store all iteration stats here

    for i in range(ITERATIONS):
        print(f"Iteration {i+1} of {ITERATIONS}")
        start_time = time.time()

        # Enqueue tasks
        for operation, count in operations:
            for _ in range(count):
                tasks.put((operation, 0), block=False)

        # Wait for current batch of tasks to finish
        print("Waiting for all tasks to be done...")
        tasks.join()
        print("All tasks done.")

        end_time = time.time()

        # Collect iteration results
        operation_count = defaultdict(
            lambda: {"success": [0, []], "failure": [0, []]})
        while not results.empty():
            operation, success, time_taken = results.get()
            results.task_done()
            if success:
                operation_count[operation]["success"][0] += 1
                operation_count[operation]["success"][1].append(time_taken)
            else:
                operation_count[operation]["failure"][0] += 1
                operation_count[operation]["failure"][1].append(time_taken)

        # Calculate stats for this iteration
        stats = calculate_stats(
            operations, operation_count, end_time - start_time)
        iteration_stats.append(stats)
        print_stats(stats)

    executor.shutdown(wait=True)

    # === Post-run Summary ===
    print("\n=== Summary of All Iterations ===")

    def collect_field(field):
        return [s[field] for s in iteration_stats]

    def collect_op_field(op_name, key):
        return [s['operation_details'][op_name][key] for s in iteration_stats]

    global_summary = {
        "average_time_mean": statistics.mean(collect_field('average_time')),
        "average_time_std": statistics.stdev(collect_field('average_time')) if ITERATIONS > 1 else 0.0,
        "total_success_mean": statistics.mean(collect_field('total_success')),
        "total_success_std": statistics.stdev(collect_field('total_success')) if ITERATIONS > 1 else 0.0,
        "total_failure_mean": statistics.mean(collect_field('total_failure')),
        "total_failure_std": statistics.stdev(collect_field('total_failure')) if ITERATIONS > 1 else 0.0,
        "operation_summaries": {}
    }

    for op, _ in operations:
        name = op.__name__
        avg_times = collect_op_field(name, 'average_time')
        mean_time = statistics.mean(avg_times)
        std_time = statistics.stdev(avg_times) if len(avg_times) > 1 else 0.0

        success_counts = collect_op_field(name, 'success_count')
        failure_counts = collect_op_field(name, 'failure_count')

        mean_success = statistics.mean(success_counts)
        std_success = statistics.stdev(success_counts) if len(
            success_counts) > 1 else 0.0

        mean_failure = statistics.mean(failure_counts)
        std_failure = statistics.stdev(failure_counts) if len(
            failure_counts) > 1 else 0.0

        print(f"\nOperation: {name}")
        print(f"\tMean time: {mean_time:.2f} s")
        print(f"\tStd dev time: {std_time:.2f} s")
        print(f"\tMean success count: {mean_success:.2f}")
        print(f"\tStd dev success count: {std_success:.2f}")
        print(f"\tMean failure count: {mean_failure:.2f}")
        print(f"\tStd dev failure count: {std_failure:.2f}")

        global_summary["operation_summaries"][name] = {
            "mean_time": mean_time,
            "std_time": std_time,
            "mean_success": mean_success,
            "std_success": std_success,
            "mean_failure": mean_failure,
            "std_failure": std_failure
        }
    # ===

    if SAVE_STATS:
        log_summary_stats(global_summary, conf)
        print(f"Global summary saved to {EXPERIMENT_DEFAULT_NAME}.csv")


if __name__ == "__main__":
    main()
