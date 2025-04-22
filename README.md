# Master thesis srcrips
## Files
- append_only_conflicts.py: Tries to append two rows at the concurrently with only one different column (bob,47) and (bob,23), Delta lake appends both of them as they are independent, the column values are not checked. This experiments verifies that appends with no schema changes can be done concurrently.
- change_schema.py: Tries to change the schema of a table concurrently, two transactions at the same time try to rename the column 'name' to 'first_name' and 'last_name'. Delta lake does not allow this operation, the first transaction that tries to change the schema is successful, the second one fails.
- merge_table_exp.py: This experiment shows how delta lake storage evolves when the schema is modified. It also shows how the rows before updating the schema are handle. For each schema Delta lake create a parquet file (only headers) and for each row a parquet file with the row data. If a new column is added the parquet files with the old schema are kept (they are not duplicated with the new header, delta lake will handle his when reading the log). Fields will be set to null.
- read_lineazability.py: this experiments shows how Delta lake handles read lineazability. In the experiment multiple `read` transactions are beeing executed at the same time with a `write` opperation. 
schema-conflicts.py
- test-custom-delta.py: this experiment is trying to use my custom version of delta lake spark.

## How to run custom delta lake
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

After installing it compile the project:
```bash
cd delta
build/sbt compile
```

To generate artifacts, run
```bash
build/sbt package
```
 After this command you will read where the `.jar`files are generated.

 On the line `9` of the `test-custom-delta.py` file you have to change the path to the jar file generated in the previous step.
 After this you can run the script with:
```bash
python3 test-custom-delta.py
```
