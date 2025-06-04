#!/bin/bash

# This script automates running the bench.py script with a fixed number of readers
# and varying numbers of writer threads and writer schema changes, such that
# the total number of writer threads remains constant at MAX_WRITERS.

# Configuration
ITERATIONS=10
NUM_READERS=0
MAX_WRITERS=15

# Official experiment
experiment_name="writers-distribution-no-retry-official-$ITERATIONS"
rm "$experiment_name".csv

# Varying num-writer-schema-change from 0 to MAX_WRITERS
for NUM_WRITER_SCHEMA_CHANGE in $(seq 0 $MAX_WRITERS); do
    NUM_WRITERS=$((MAX_WRITERS - NUM_WRITER_SCHEMA_CHANGE))
    # Rub n iterations with the current configuration
    for i in $(seq 1 $ITERATIONS); do
    ./run_official.sh bench.py --iterations 1 --num-readers $NUM_READERS \
                                --num-writers $NUM_WRITERS --num-writer-schema-change $NUM_WRITER_SCHEMA_CHANGE \
                                --save --name "$experiment_name"
    done
done

# Custom experiment
experiment_name="writers-distribution-no-retry-custom-$ITERATIONS"
rm "$experiment_name".csv

for NUM_WRITER_SCHEMA_CHANGE in $(seq 0 $MAX_WRITERS); do
    NUM_WRITERS=$((MAX_WRITERS - NUM_WRITER_SCHEMA_CHANGE))
    ./run_custom.sh bench.py --iterations $ITERATIONS --num-readers $NUM_READERS \
                              --num-writers $NUM_WRITERS --num-writer-schema-change $NUM_WRITER_SCHEMA_CHANGE \
                              --save --name "$experiment_name"
done
