import pandas as pd
import matplotlib.pyplot as plt
import sys

if len(sys.argv) < 2:
    print("Usage: python writer_reader_chart.py <data_file.csv>")
    sys.exit(1)

# Load the data from CSV
csv_file = sys.argv[1]
df = pd.read_csv(csv_file)

# Calculate total append success and failure counts
df['total_append_success'] = (
    df['success_count_append_delta_row'] +
    df['success_count_change_schema_and_append_delta_row']
)
df['total_append_failure'] = (
    df['failure_count_append_delta_row'] +
    df['failure_count_change_schema_and_append_delta_row']
)
df['total_append_attempts'] = df['total_append_success'] + \
    df['total_append_failure']

# Compute failure percentage
df['append_failure_pct'] = (
    df['total_append_failure'] / df['total_append_attempts']) * 100

# Group by number of writers and average the failure percentage
grouped = df.groupby('num_writers')['append_failure_pct'].mean().reset_index()


grouped['num_writers'] = grouped['num_writers'] * 2
# Plotting a bar chart
plt.figure(figsize=(10, 6))
plt.bar(grouped['num_writers'], grouped['append_failure_pct'], color='salmon')
plt.xlabel('Number of Concurrent Writers')
plt.ylabel('Append Failure Percentage (%)')
plt.title(
    'Total Append Failure Percentage vs Number of Writers (half change the schema)')
plt.xticks(grouped['num_writers'])  # Ensure all bars are labeled
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()

# Save and show the plot
plt.savefig('writer_conflfict_prob.png')
plt.show()
