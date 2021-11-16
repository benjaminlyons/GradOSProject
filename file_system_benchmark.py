import time
from dask.distributed import Client, as_completed
import sys

def read_file():
        f = open("/scratch365/blyons1/grados-project/bible2.txt")
        data = f.read()
        count = len(data)
        f.close()

f = open("filesystem.csv", "a")
num_machines = int(sys.argv[1])
print("machines:", num_machines)
num_cores = 4*num_machines # each machine has 4 cores
print("cores:", num_cores)

client = Client("tcp://10.32.85.31:8790")
start = time.time()
results = []
for i in range(num_cores):
        result = client.submit(read_file)
        results.append(result)
client.gather(results)
stop = time.time()

throughput = num_cores / (stop - start)
print("Throughput:", throughput)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()

