import pandas as pd
import matplotlib.pyplot as plt
import sys

if len(sys.argv) < 3:
    print("Usage: python implementation-differences.py <official_file.csv> <custom_file.csv>")
    sys.exit(1)

# Load the data from CSV
official_csv_file = sys.argv[1]
custom_csv_file = sys.argv[2]

official_df = pd.read_csv(official_csv_file)
custom_df = pd.read_csv(custom_csv_file)


# -- PLOT 1 time taken vs num_writers --
# Group by num_writers and calculate average total_time_taken
official_grouped = official_df.groupby(
    "num_writers")["total_time_taken"].mean().reset_index()
custom_grouped = custom_df.groupby(
    "num_writers")["total_time_taken"].mean().reset_index()

# Merge the dataframes to align num_writers
merged_df = pd.merge(official_grouped, custom_grouped,
                     on="num_writers", suffixes=('_official', '_custom'))

# Bar positions
num_items = len(merged_df)
bar_width = 0.4
x_labels = list(merged_df["num_writers"])
x_positions = list(range(num_items))
x_positions_official = [x - bar_width / 2 for x in x_positions]
x_positions_custom = [x + bar_width / 2 for x in x_positions]

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(x_positions_official,
        merged_df["total_time_taken_official"], width=bar_width, label='Official', color='skyblue')
plt.bar(x_positions_custom,
        merged_df["total_time_taken_custom"], width=bar_width, label='Custom', color='salmon')

# Labeling
plt.xlabel('Number of Writers')
plt.ylabel('Average Total Time Taken')
plt.title('Total Time Taken vs Number of Writers')
plt.xticks(x_positions, x_labels)
plt.legend()
plt.grid(True, axis='y')
plt.tight_layout()

# Show plot
plt.savefig("implementation-time-differences.png")
plt.show()

# -- PLOT 2 errors vs num_writers --
# Group official data by num_writers and sum failure counts
failures_grouped = official_df.groupby("num_writers")[[
    "total_failure",
    "failure_count_change_schema_and_append_delta_row"
]].sum().reset_index()

# Bar positions
num_items = len(failures_grouped)
bar_width = 0.4
x_labels = list(failures_grouped["num_writers"])
x_positions = list(range(num_items))
x_positions_total = [x - bar_width / 2 for x in x_positions]
x_positions_schema = [x + bar_width / 2 for x in x_positions]

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(x_positions_total,
        failures_grouped["total_failure"], width=bar_width, label='Total Failures', color='gray')
plt.bar(x_positions_schema,
        failures_grouped["failure_count_change_schema_and_append_delta_row"], width=bar_width, label='Schema Change Failures', color='orange')

# Labeling
plt.xlabel('Number of Writers')
plt.ylabel('Number of Failures')
plt.title('Failure Comparison vs Number of Writers (Official)')
plt.xticks(x_positions, x_labels)
plt.legend()
plt.grid(True, axis='y')
plt.tight_layout()

# Save and show
plt.savefig("implementation-failure-differences.png")
plt.show()
