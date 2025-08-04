#!/bin/bash
# This script runs a Python script with a specified number of threads.
for threads in {1..8}; do
    for i in {1..10}; do
        ./run_custom_with_threads.sh bench-experiments/json_deserialization.py  $threads
    done
done
