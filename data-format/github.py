import os
import json
from glob import glob
from collections import defaultdict

INPUT_DIR = 'data/'
DATE_PREFIX = '2015-01-01'
OUTPUT_DIR = 'data/github_by_type/'

# Open one file handle per type for writing
file_handles = defaultdict(lambda: None)


def get_type(json_obj):
    return json_obj.get("type")


try:
    # Go through all files matching the pattern
    for file_path in glob(os.path.join(INPUT_DIR, f"{DATE_PREFIX}-*.json")):
        print(f"Processing {file_path}")
        with open(file_path, 'r') as infile:
            for line in infile:
                if not line.strip():
                    continue  # Skip empty lines
                try:
                    obj = json.loads(line)
                    obj_type = get_type(obj)
                    if obj_type is None:
                        continue  # Skip if no "type" field
                    # Open the output file for this type if not already open
                    if file_handles[obj_type] is None:
                        out_path = os.path.join(
                            OUTPUT_DIR, f"{DATE_PREFIX}-{obj_type}.json")
                        file_handles[obj_type] = open(out_path, 'a')
                    # Write the object to the corresponding file
                    file_handles[obj_type].write(json.dumps(obj) + '\n')
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {file_path}: {e}")
finally:
    # Close all open file handles
    for f in file_handles.values():
        if f:
            f.close()
