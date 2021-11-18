import sys
import matplotlib.pyplot as plt
import pandas as pd
import csv
import numpy as np

titles = ["Reduction", "Shared", "Sequential", "Parallel"]

def extract_data(filename):
        with open(filename) as f:
                reader = csv.reader(f)
                labels = next(reader)
                data = []
                for row in reader:
                        data.append([float(x) for x in row])
                data = np.array(data)
                data = data[data[:,0].argsort()]
        return data

fig, axs = plt.subplots(1, 1)
title = "Shared file system"
ax = axs
data = extract_data("filesystem.csv")

x = [ d[0] for d in data ]
y1 = [ d[1] for d in data ]

ax.scatter(x, y1, label="small")
ax.set_title(title)
ax.set_xlabel('cores')
ax.set_ylabel('throughput (tasks/sec)')
ax.set_xlim([0, max(x) + 10])
ax.set_ylim([0, max(y1) + max(y1)*.1])

plt.tight_layout()
plt.show()
plt.savefig("filesystem.pdf")


