import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load CSV
# Change filename if needed
df = pd.read_csv("json_serialization_pool_times.csv")

# Sort by num_threads just in case
df = df.sort_values("num_threads")

# Bar settings
x = np.arange(len(df["num_threads"]))
bar_width = 0.35

# Plot
plt.figure(figsize=(10, 6))
plt.bar(x - bar_width/2, df["write_values_mean"],
        yerr=df["write_values_stdev"], capsize=5, width=bar_width, label="Write Values")
plt.bar(x + bar_width/2, df["write_strings_mean"],
        yerr=df["write_strings_stdev"], capsize=5, width=bar_width, label="Write Strings")

# Labels and formatting
plt.xlabel("Number of Threads")
plt.ylabel("Time (seconds)")
plt.title("Write Times (Mean ± Std Dev)")
plt.xticks(x, df["num_threads"])
plt.legend()
plt.grid(axis="y", linestyle="--", alpha=0.6)
plt.tight_layout()

# Save
plt.savefig("write_pool_means_chart.png")
plt.show()
plt.close()
