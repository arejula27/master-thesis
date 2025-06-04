
import pandas as pd
import matplotlib.pyplot as plt
import sys

if len(sys.argv) < 2:
    print("Usage: python writer_reader_chart.py <data_file.csv>")
    sys.exit(1)

# Load the data
csv_file = sys.argv[1]
df = pd.read_csv(csv_file)

# Compute total append failures
df['total_append_failure'] = (
    df['failure_count_append_delta_row'] +
    df['failure_count_change_schema_and_append_delta_row']
)

# Compute total writer threads
df['total_writers'] = df['num_writers'] + df['num_writer_schema_change']

# Filter out rows with zero writers
df = df[df['total_writers'] > 0]

# Compute schema-change writer percentage
df['schema_change_pct'] = (
    df['num_writer_schema_change'] / df['total_writers']) * 100

# Normalize total_append_failure to mean per iteration
df['append_failure_per_iter'] = df['total_append_failure']

# Group by schema_change_pct and compute mean and std
grouped = df.groupby('schema_change_pct')['append_failure_per_iter'].agg(
    ['mean', 'std']).reset_index()
grouped['schema_change_pct'] = grouped['schema_change_pct'].round(
    0).astype(int)

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(grouped['schema_change_pct'], grouped['mean'],
        yerr=grouped['std'], capsize=5, color='steelblue')
print(grouped)
plt.xlabel('Percentage of Writers Changing Schema')
plt.ylabel('Mean Append Failures per Iteration (± Std Dev)')
plt.title('Mean Append Failures per Iteration vs % of Schema-Change Writers')
plt.xticks(grouped['schema_change_pct'])  # Show exact percentages
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()

# Save and show
plt.savefig('schema_change_distribution_mean_std.png')
plt.show()
