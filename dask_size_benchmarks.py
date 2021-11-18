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

client = Client("tcp://10.32.85.31:8791")
def func(l):
        time.sleep(0.1)
        return l[0]

# define small list size
N_small = 25
N_medium = 4000 
N_large = 25000

data_small = [ i for i in range(N_small) ]
data_medium = [ i for i in  range(N_medium) ]
data_large = [ i for i in range(N_large) ]

# small
num_tasks_small = 200*num_cores
futures = []
start = time.time()
for i in range(num_tasks_small):
        data_small[0] = i
        future = client.submit(func, data_small)
        futures.append(future)
client.gather(futures)
stop = time.time()
throughput_small = num_tasks_small / (stop - start)
        
# medium
num_tasks_medium = 10*num_cores
futures = []
start = time.time()
for i in range(num_tasks_medium):
        data_medium[0] = i
        future = client.submit(func, data_medium)
        futures.append(future)
client.gather(futures)
stop = time.time()
throughput_medium = num_tasks_medium / (stop - start)

# large
num_tasks_large = 4*num_cores
futures = []
start = time.time()
for i in range(num_tasks_large):
        data_large[0] = i
        future = client.submit(func, data_large)
        futures.append(future)
client.gather(futures)
stop = time.time()
throughput_large = num_tasks_large / (stop - start)

f = open("size_benchmarks.csv", "a")
f.write(str(num_cores) + "," + str(throughput_small) + "," + str(throughput_medium) + "," + str(throughput_large) + "\n")
print(str(num_cores) + "," + str(throughput_small) + "," + str(throughput_medium) + "," + str(throughput_large) + "\n")
f.close()

