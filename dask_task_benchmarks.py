import time
import sys
from dask.distributed import Client, wait
import dask.array as da
import math

num_cores = int(sys.argv[1])
print("machines:", num_cores)

# inspired in part by https://matthewrocklin.com/blog/work/2017/07/03/scaling
def add(a, b):
        return a + b

def increment(x):
        return x + 1

client = Client("tcp://10.32.85.47:8790")

# parallel calculations
N = 2**10*num_cores
start = time.time()
futures = client.map(increment, range(N))
result = client.gather(futures)
stop = time.time()
f = open("increment.csv", "a")
throughput = N / (stop - start)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()
print("Increment:", str(throughput), 's')

# tree reduction - sum list
N = int(5000 * math.sqrt(num_cores))
data = da.random.randint(0, 10000, size=(N,), chunks=(N//num_cores,))
start = time.time()
result = data.sum().compute()
stop = time.time()
f = open("sum.csv", "a")
throughput = N / (stop - start)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()
print("Sum list:", str(throughput), 's')

# some shared data
data = range(2**10*num_cores)
start = time.time()
data = client.map(add, data[:-1], data[1:])
data = client.map(add, data[:-1], data[1:])
result = client.gather(data)
stop = time.time()
f = open("adds.csv", "a")
throughput = N / (stop - start)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()
print("Shared Adds:", str(throughput), 's')

# purely sequential calculations
a = 1
N = 1000
start = time.time()
for i in range(N):
        a = client.submit(increment, a)
a = client.gather(a)
stop = time.time()
f = open("sequential.csv", "a")
throughput = N / (stop - start)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()
print("Sequential:", str(throughput), 's')



