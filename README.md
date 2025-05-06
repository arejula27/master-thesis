# Master thesis srcrips
## Files
- append_only_conflicts.py: Tries to append two rows at the concurrently with only one different column (bob,47) and (bob,23), Delta lake appends both of them as they are independent, the column values are not checked. This experiments verifies that appends with no schema changes can be done concurrently.
- change_schema.py: Tries to change the schema of a table concurrently, two transactions at the same time try to rename the column 'name' to 'first_name' and 'last_name'. Delta lake does not allow this operation, the first transaction that tries to change the schema is successful, the second one fails.
- merge_table_exp.py: This experiment shows how delta lake storage evolves when the schema is modified. It also shows how the rows before updating the schema are handle. For each schema Delta lake create a parquet file (only headers) and for each row a parquet file with the row data. If a new column is added the parquet files with the old schema are kept (they are not duplicated with the new header, delta lake will handle his when reading the log). Fields will be set to null.
- read_lineazability.py: this experiments shows how Delta lake handles read lineazability. In the experiment multiple `read` transactions are beeing executed at the same time with a `write` opperation. 
schema-conflicts.py
- test-custom-delta.py: this experiment is trying to use my custom version of delta lake spark.

## How to build custom delta lake
1. Clone the delta lake repository
```bash
git clone git@github.com:delta-io/delta.git 
```
It is required to have installed `sbt` and `java 8` in your system, if you do not want to install them locally you can use the nix dev environment .
```bash
cp shell.nix ~/delta/shell.nix
cd ~/delta
nix-shell
```
2. Build the project on the version `3.1.0`
```bash
git checkout v3.1.0
build/sbt compile
```

To generate artifacts, run
```bash
build/sbt package
```
 After this command you will read where the `.jar`files are generated. You need to:
- `delta/spark/target/scala-2.12/delta-spark_2.12-3.1.0.jar`
- `delta/storage/target/delta-storage-3.1.0.jar`

# How to run the custom delta lake with spark
I provided a `nix-shell` environment to run the experiments. You can use it to run the experiment, ensuring that you will have the same environment as me. You can also run the experiments with your own environment, but you need to have installed `python3.13`, `Java8`, `sbt` and `spark 3.5.0`. If you do not have Nix installed check the [official Nix installation guide](https://nixos.org/download). If you want to work with the `nix-shell` environment run:
```bash
nix-shell
```

1. Create a virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```
2. Install the required packages
```bash
pip install -r requirements.txt
```
The delta lake `jar` files should be in the `delta-jars` folder. If you do not have this folder, you can create it and copy the files from the previous step.

3. Run the script, the two versions supports by the default is the official `v3.1.0` and the custom one based on `v3.1.0`. In case you want to run your compiled version you need to modify the `run_custom.sh`, changing the file name of the `jar` files.

To check that everything works you can run the official version with any python script:
```bash
./run_official.sh exploration-experiments/create-table.py
```
You can also run the custom version with the same script:
```bash
./run_custom.sh exploration-experiments/create-table.py
```
