import pandas as pd
import matplotlib.pyplot as plt
import sys

if len(sys.argv) < 2:
    print("Usage: python compare_success_failure.py <official_file.csv>")
    sys.exit(1)

official_file = sys.argv[1]

df = pd.read_csv(official_file)

# Columns of interest
columns = [
    "num_writers",
    "append_delta_row_mean_success",
    "append_delta_row_std_success",
    "append_delta_row_mean_failure",
    "append_delta_row_std_failure",
    "change_schema_and_append_delta_row_mean_success",
    "change_schema_and_append_delta_row_std_success",
    "change_schema_and_append_delta_row_mean_failure",
    "change_schema_and_append_delta_row_std_failure"
]

df = df[columns]

# Group by num_writers and average
grouped = df.groupby("num_writers").mean().reset_index()

num_items = len(grouped)
bar_width = 0.2
spacing = 2.5
x_positions = [i * spacing for i in range(num_items)]

# x positions for bars
x_append_success = [x - 1.5 * bar_width for x in x_positions]
x_append_failure = [x - 0.5 * bar_width for x in x_positions]
x_schema_success = [x + 0.5 * bar_width for x in x_positions]
x_schema_failure = [x + 1.5 * bar_width for x in x_positions]

plt.figure(figsize=(14, 7))

# Append delta row success and failure bars
plt.bar(
    x_append_success,
    grouped["append_delta_row_mean_success"],
    yerr=grouped["append_delta_row_std_success"],
    width=bar_width,
    capsize=4,
    label="Append Success",
    color="#4A90E2",
)
plt.bar(
    x_append_failure,
    grouped["append_delta_row_mean_failure"],
    yerr=grouped["append_delta_row_std_failure"],
    width=bar_width,
    capsize=4,
    label="Append Failure",
    color="#7FB3D5",
)

# Change schema and append success and failure bars
plt.bar(
    x_schema_success,
    grouped["change_schema_and_append_delta_row_mean_success"],
    yerr=grouped["change_schema_and_append_delta_row_std_success"],
    width=bar_width,
    capsize=4,
    label="Schema+Append Success",
    color="#D35400",
)
plt.bar(
    x_schema_failure,
    grouped["change_schema_and_append_delta_row_mean_failure"],
    yerr=grouped["change_schema_and_append_delta_row_std_failure"],
    width=bar_width,
    capsize=4,
    label="Schema+Append Failure",
    color="#E59866",
)

plt.xlabel("Number of Writers")
plt.ylabel("Rate")
plt.title("Success and Failure Rate Comparison: Append vs Schema+Append")
plt.xticks(x_positions, grouped["num_writers"] * 2)
plt.legend(loc="upper right", fontsize="small")
plt.grid(axis="y", linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig("success_failure_comparison.png")
plt.show()
