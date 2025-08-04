
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load CSV
df = pd.read_csv("json_serialization_times.csv")

# Group by num_threads and calculate mean and std for write times
write_grouped = df.groupby("num_threads").agg(
    write_values_mean=("write_values_time", "mean"),
    write_values_std=("write_values_time", "std"),
    write_strings_mean=("write_strings_time", "mean"),
    write_strings_std=("write_strings_time", "std"),
).reset_index()

# Plot Write Times
bar_width = 0.35
x = np.arange(len(write_grouped["num_threads"]))

plt.figure(figsize=(10, 6))
plt.bar(x - bar_width/2, write_grouped["write_values_mean"],
        yerr=write_grouped["write_values_std"], capsize=5, width=bar_width, label="Write Values")
plt.bar(x + bar_width/2, write_grouped["write_strings_mean"],
        yerr=write_grouped["write_strings_std"], capsize=5, width=bar_width, label="Write Strings")
plt.xlabel("Number of Threads")
plt.ylabel("Time (seconds)")
plt.title("Write Times (Mean ± Std Dev)")
plt.xticks(x, write_grouped["num_threads"])
plt.legend()
plt.grid(axis="y", linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("write_json_times.png")
plt.close()

# Group by num_threads and calculate mean and std for read times
read_grouped = df.groupby("num_threads").agg(
    read_values_mean=("read_values_time", "mean"),
    read_values_std=("read_values_time", "std"),
    read_strings_mean=("read_strings_time", "mean"),
    read_strings_std=("read_strings_time", "std"),
).reset_index()

# Plot Read Times
x = np.arange(len(read_grouped["num_threads"]))

plt.figure(figsize=(10, 6))
plt.bar(x - bar_width/2, read_grouped["read_values_mean"],
        yerr=read_grouped["read_values_std"], capsize=5, width=bar_width, label="Read Values")
plt.bar(x + bar_width/2, read_grouped["read_strings_mean"],
        yerr=read_grouped["read_strings_std"], capsize=5, width=bar_width, label="Read Strings")
plt.xlabel("Number of Threads")
plt.ylabel("Time (seconds)")
plt.title("Read Times (Mean ± Std Dev)")
plt.xticks(x, read_grouped["num_threads"])
plt.legend()
plt.grid(axis="y", linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("read_json_times.png")
plt.show()
plt.close()
