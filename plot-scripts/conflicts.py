import pandas as pd
import matplotlib.pyplot as plt
import sys

if len(sys.argv) < 2:
    print("Usage: python writer_reader_chart.py <data_file.csv>")
    sys.exit(1)

# Load the data from CSV
csv_file = sys.argv[1]
df = pd.read_csv(csv_file)


# --- Plot 1: Total combined failure percentage ---
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
# print appends failure delta row and with schema change

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

# --- Plot 2 with successes and failures ---

# Group sums of success and failure counts by num_writers
grouped_counts = df.groupby('num_writers')[[
    'success_count_append_delta_row',
    'failure_count_append_delta_row',
    'success_count_change_schema_and_append_delta_row',
    'failure_count_change_schema_and_append_delta_row'
]].sum().reset_index()

grouped_counts['num_writers'] = grouped_counts['num_writers'] * 2

plt.figure(figsize=(12, 7))
width = 0.2
x = grouped_counts['num_writers']

plt.bar(x - 1.5*width, grouped_counts['success_count_append_delta_row'],
        width, label='Success Append Without Schema Change', color='lightgreen')
plt.bar(x - 0.5*width, grouped_counts['failure_count_append_delta_row'],
        width, label='Failure Append Without Schema Change', color='salmon')
plt.bar(x + 0.5*width, grouped_counts['success_count_change_schema_and_append_delta_row'],
        width, label='Success Append With Schema Change', color='skyblue')
plt.bar(x + 1.5*width, grouped_counts['failure_count_change_schema_and_append_delta_row'],
        width, label='Failure Append With Schema Change', color='orange')

plt.xlabel('Number of Concurrent Writers')
plt.ylabel('Count')
plt.title(
    'Comparison of Successes and Failures in Appends (Without vs With Schema Change)')
plt.xticks(x)
plt.legend()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig('append_success_failure_comparison.png')
plt.show()
