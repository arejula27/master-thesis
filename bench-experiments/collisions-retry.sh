#!/bin/bash

# This script automates running the bench.py script with a fixed number of readers
# and varying numbers of writers. The number of iterations is fixed at 10.

rm "writers-conflict-retry-official.csv"
# Configuration
ITERATIONS=10
NUM_READERS=3
MAX_WRITERS=7

# Run the experiment for each writer count from 0 to MAX_WRITERS
for NUM_WRITERS in $(seq 0 $MAX_WRITERS); do
    ./run_official.sh bench.py --retry-failed --iterations $ITERATIONS --num-readers $NUM_READERS \
                              --num-writers $NUM_WRITERS --num-writer-schema-change $NUM_WRITERS --save --name "writers-conflict-retry-official" 
done
