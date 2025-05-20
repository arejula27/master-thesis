import pandas as pd
import matplotlib.pyplot as plt
import sys

if len(sys.argv) < 2:
    print("Usage: python writer_reader_chart.py <data_file.csv>")
    sys.exit(1)

# Load the data from CSV
csv_file = sys.argv[1]
data = pd.read_csv(csv_file)

# Prepare the data for plotting
num_writers = data['num_writers']
avg_append_time = data['average_time_append_delta_row']
avg_read_time = data['average_time_read_delta_table']

# Set up the column plot
plt.figure(figsize=(14, 8))
bar_width = 0.35
indices = range(len(num_writers))


plt.bar(indices, avg_append_time, width=bar_width,
        color='skyblue', label='Average Append Time (Writers)')
plt.bar([i + bar_width for i in indices], avg_read_time,
        width=bar_width, color='salmon', label='Average Read Time (Readers)')

# Add labels and title
plt.title('Average Operation Time by Number of Writers')
plt.xlabel('Number of Writers')
plt.ylabel('Average Time (ms)')
plt.xticks([i + bar_width / 2 for i in indices], num_writers, rotation=45)
plt.legend()
plt.tight_layout()

# Show the plot
# Guardar el gráfico en archivo PNG
plt.savefig('writers_no_conflict.png')
