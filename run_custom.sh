#! /bin/bash



# Check it has at least one argument
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <path_to_python_script>"
    exit 1
fi

# Get the file name from the argument (all the args from the first)

python_script="${@:1}"


# Check if the argument is a file
if [ ! -f "$1" ]; then
    echo "Error: $1 is not a file"
    exit 1
fi
# Check if the file is a python script
if [[ "$1" != *.py ]]; then
    echo "Error: $1 is not a python script"
    exit 1
fi

# Jars path
jars_path="delta-jars"
# Delta jars files required
jars_files=(
    "custom-delta-spark_2.12-3.3.1.jar"
    "custom-delta-storage-3.3.1.jar"
)
# Check if the JAR files exist
for jar in "${jars_files[@]}"; do
    if [ ! -f "$jars_path/$jar" ]; then
	echo "Error: $jars_path/$jar does not exist"
	exit 1
    fi
done
# create a string with the path+file for each jar
delta_jars=""
for jar in "${jars_files[@]}"; do
    delta_jars+="$jars_path/$jar,"
done


spark-submit \
  --class DeltaExperiment \
  --jars ${delta_jars%?} \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.databricks.delta.schema.autoMerge.enabled=true" \
  $python_script 2> /dev/null
