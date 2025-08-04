#!/bin/bash

# This script automates running the bench.py script with a fixed number of readers
# and varying numbers of writers. The number of iterations is fixed at 10.

# Configuration
ITERATIONS=10
NUM_READERS=3
MAX_WRITERS=7

experiment_name="writers-conflict-no-retry-official-$ITERATIONS"
rm "$experiment_name".csv

# Run the experiment for each writer count from 0 to MAX_WRITERS
for NUM_WRITERS in $(seq 0 $MAX_WRITERS); do
    ./run_official.sh bench.py --iterations $ITERATIONS --num-readers $NUM_READERS \
                              --num-writers $NUM_WRITERS --num-writer-schema-change $NUM_WRITERS --save --name "$experiment_name"
done

experiment_name="writers-conflict-no-retry-custom-$ITERATIONS"
rm "$experiment_name".csv

# Same with my custom delta lake
for NUM_WRITERS in $(seq 0 $MAX_WRITERS); do
    ./run_custom.sh bench.py --iterations $ITERATIONS --num-readers $NUM_READERS \
                              --num-writers $NUM_WRITERS --num-writer-schema-change $NUM_WRITERS --save --name "$experiment_name"
done
