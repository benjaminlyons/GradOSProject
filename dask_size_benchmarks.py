import time
import sys
from dask.distributed import Client, wait
from dask import delayed
import dask.array as da
import math

num_machines = int(sys.argv[1])
print("machines:", num_machines)
num_cores = 4*num_machines # each machine has 4 cores
print("cores:", num_cores)

def sum_list(l):
        time.sleep(0.1)
        return sum(l)

# define small list size
N_small = 1000 # 8 KB?
N_medium = 1000*N_small # 8 MB?
N_large = 20*N_medium # 160 MB

data_small = range(N_small)
data_medium = range(N_medium)
data_large = range(N_large)

# small
num_tasks_small = 200*num_cores
futures = []
start = time.time()
for i in range(num_tasks_small):
        future = client.submit(sum_list, data_small)
        futures.append(future)
client.gather(futures)
stop = time.time()
throughput_small = num_tasks_small / (stop - start)
        
# medium
num_tasks_medium = 10*num_cores
futures = []
start = time.time()
for i in range(num_tasks_medium):
        future = client.submit(sum_list, data_medium)
        futures.append(future)
client.gather(futures)
stop = time.time()
throughput_medium = num_tasks_medium / (stop - start)

# large
num_tasks_large = 4*num_cores
futures = []
start = time.time()
for i in range(num_tasks_large):
        future = client.submit(sum_list, data_large)
        futures.append(future)
client.gather(futures)
stop = time.time()
throughput_large = num_tasks_large / (stop - start)

f = open("size_benchmarks.csv", "a")
f.write(str(num_cores) + "," + str(throughput_small) + "," + str(throughput_medium) + "," + str(throughput_large) + "\n")
print(str(num_cores) + "," + str(throughput_small) + "," + str(throughput_medium) + "," + str(throughput_large) + "\n")
f.close()

