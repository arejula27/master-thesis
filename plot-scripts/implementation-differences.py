import pandas as pd
import matplotlib.pyplot as plt
import sys

if len(sys.argv) < 3:
    print("Usage: python implementation-differences.py <official_file.csv> <custom_file.csv>")
    sys.exit(1)

# Load the data
official_csv_file = sys.argv[1]
custom_csv_file = sys.argv[2]

official_df = pd.read_csv(official_csv_file)
custom_df = pd.read_csv(custom_csv_file)

# Relevant columns
columns = [
    "num_writers",
    "average_time_mean",
    "append_delta_row_mean_time",
    "append_delta_row_std_time",
    "change_schema_and_append_delta_row_mean_time",
    "change_schema_and_append_delta_row_std_time"
]

# Group by num_writers and average
official_grouped = official_df[columns].groupby(
    "num_writers").mean().reset_index()
custom_grouped = custom_df[columns].groupby("num_writers").mean().reset_index()

# Merge by num_writers
merged = pd.merge(official_grouped, custom_grouped,
                  on="num_writers", suffixes=("_official", "_custom"))


# Bar positions
num_items = len(merged)
bar_width = 0.2

# Aumentar separación entre grupos
spacing = 2.0
x_positions = [i * spacing for i in range(num_items)]
x_append_official = [x - 2.5 * bar_width for x in x_positions]
x_schema_official = [x - 1.5 * bar_width for x in x_positions]
x_avg_official = [x - 0.5 * bar_width for x in x_positions]

x_append_custom = [x + 0.5 * bar_width for x in x_positions]
x_schema_custom = [x + 1.5 * bar_width for x in x_positions]
x_avg_custom = [x + 2.5 * bar_width for x in x_positions]

# Plotting
plt.figure(figsize=(14, 6))

# Official implementation bars
# append only
plt.bar(
    x_append_official,
    merged["append_delta_row_mean_time_official"],
    yerr=merged["append_delta_row_std_time_official"],
    capsize=5,
    width=bar_width,
    label="Append - Official",
    color="#4A90E2"
)
# change schema and append
plt.bar(
    x_schema_official,
    merged["change_schema_and_append_delta_row_mean_time_official"],
    yerr=merged["change_schema_and_append_delta_row_std_time_official"],
    capsize=5,
    width=bar_width,
    label="Schema Change + Append - Official",
    color="#003366"
)


# average time
plt.bar(
    x_avg_official,
    merged["average_time_mean_official"],
    width=bar_width,
    label="Total Time - Official",
    color="#7ED6DF"
)
# Custom implementation bars
# append only
plt.bar(
    x_append_custom,
    merged["append_delta_row_mean_time_custom"],
    yerr=merged["append_delta_row_std_time_custom"],
    capsize=5,
    width=bar_width,
    label="Append - Custom",
    color="#F5A623"
)
# change schema and append
plt.bar(
    x_schema_custom,
    merged["change_schema_and_append_delta_row_mean_time_custom"],
    yerr=merged["change_schema_and_append_delta_row_std_time_custom"],
    capsize=5,
    width=bar_width,
    label="Schema Change + Append - Custom",
    color="#D35400"
)

# average time
plt.bar(
    x_avg_custom,
    merged["average_time_mean_custom"],
    width=bar_width,
    label="Total Time - Custom",
    color="#E67E22"
)

# Labeling
plt.xlabel("Number of Writers")
plt.ylabel("Time (s)")
plt.title("Time Comparison")
plt.xticks(x_positions, merged["num_writers"]*2)
plt.legend()
plt.grid(True, axis="y")
plt.tight_layout()

# Save and show
plt.savefig("time_comparison.png")
plt.show()
