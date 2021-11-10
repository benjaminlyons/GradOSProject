import sys
import matplotlib.pyplot as plt
import pandas as pd
import csv
import numpy as np

filenames = ["sum.csv", "adds.csv", "sequential.csv", "increment.csv"]
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

fig, axs = plt.subplots(2, 2)
for i in range(len(filenames)):
        title = titles[i]
        ax = axs[i//2][i % 2]
        data = extract_data(filenames[i])
        output_file = "{}.{}".format(filenames[i].replace('.csv', ''),'png')

        x = [ d[0] for d in data ]
        y = [ d[1] for d in data ]

        ax.plot(x, y)
        ax.set_title(title)
        ax.set_xlabel('cores')
        ax.set_ylabel('throughput (tasks/sec)')
        ax.set_xlim([0, max(x) + 10])
        ax.set_ylim([0, max(y) + max(y)*.1])

plt.tight_layout()
plt.show()
plt.savefig("output.png")


