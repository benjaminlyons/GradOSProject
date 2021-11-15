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

# inspired in part by https://matthewrocklin.com/blog/work/2017/07/03/scaling
def add(a, b):
        return a + b

def increment(x):
        return x + 1

def delayed_add(a,b):
        time.sleep(0.1)
        return a + b

def delayed_increment(x):
        time.sleep(0.1)
        return x + 1

def long_add(a,b):
        time.sleep(1)
        return a + b

def long_increment(x):
        time.sleep(1)
        return x + 1


client = Client("tcp://10.32.85.31:8791")

# parallel calculations
N_short = 2**8*num_cores
N_medium = 2**7*num_cores
N_long = 4*num_cores

#short
start = time.time()
futures = client.map(increment, range(N_short))
result = client.gather(futures)
stop = time.time()
throughput_short = N_short / (stop - start)

#medium
start = time.time()
futures = client.map(delayed_increment, range(N_medium))
result = client.gather(futures)
stop = time.time()
throughput_medium = N_medium / (stop - start)

# long
start = time.time()
futures = client.map(long_increment, range(N_long))
result = client.gather(futures)
stop = time.time()
throughput_long = N_long / (stop - start)


#output
f = open("increment.csv", "a")
f.write(str(num_cores) + "," + str(throughput_short) + "," + str(throughput_medium) + "," + str(throughput_long) + "\n")
f.close()
print("Increment:", str(throughput_short), 's')

# tree reduction - sum list
N_short = 2**7 * num_cores
data = range(N_short)

#short
start = time.time()
while len(data) > 1:
        data = [ delayed(add)(a,b) for a, b in zip(data[::2], data[1::2]) ]
print(data[0].compute())
stop = time.time()
throughput_short = N_short / (stop - start)

#medium
N_medium = 2**7 * num_cores
data = range(N_medium)
start = time.time()
while len(data) > 1:
        data = [ delayed(delayed_add)(a,b) for a, b in zip(data[::2], data[1::2]) ]
stop = time.time()
throughput_medium = N_medium / (stop - start)

#long
N_long = 4 * num_cores
data = range(N_long)
start = time.time()
while len(data) > 1:
        data = [ delayed(long_add)(a,b) for a, b in zip(data[::2], data[1::2]) ]
stop = time.time()
throughput_long = N_long / (stop - start)

#output
f = open("sum.csv", "a")
f.write(str(num_cores) + "," + str(throughput_short) + "," + str(throughput_medium) + "," + str(throughput_long) + "\n")
f.close()
print("Sum list:", str(throughput_short), 's')

# some shared data
N_short = 2**6*num_cores
data = range(N_short)

#short
start = time.time()
data = client.map(add, data[:-1], data[1:])
data = client.map(add, data[:-1], data[1:])
result = client.gather(data)
stop = time.time()
throughput_short = N_short / (stop - start)

#medium
N_medium = 2**7 * num_cores
data = range(N_medium)
start = time.time()
data = client.map(delayed_add, data[:-1], data[1:])
data = client.map(delayed_add, data[:-1], data[1:])
result = client.gather(data)
stop = time.time()
throughput_medium = N_medium / (stop - start)

# long
N_medium = 4 * num_cores
data = range(N_long)
start = time.time()
data = client.map(long_add, data[:-1], data[1:])
data = client.map(long_add, data[:-1], data[1:])
result = client.gather(data)
stop = time.time()
throughput_long = N_long / (stop - start)

#output
f = open("adds.csv", "a")
f.write(str(num_cores) + "," + str(throughput_short) + "," + str(throughput_medium) + "," + str(throughput_long) + "\n")
f.close()
print("Shared Adds:", str(throughput_short), 's')

# purely sequential calculations
a = 1
N_short = 100
start = time.time()
for i in range(N_short):
        a = client.submit(increment, a)
a = client.gather(a)
stop = time.time()
throughput_short = N_short / (stop - start)

# medium
N_medium = 100
a = 1
start = time.time()
for i in range(N_medium):
        a = client.submit(delayed_increment, a)
a = client.gather(a)
stop = time.time()
throughput_medium = N_medium / (stop - start)

# long
a = 1
N_long = 100
start = time.time()
for i in range(N_long):
        a = client.submit(long_increment, a)
a = client.gather(a)
stop = time.time()
throughput_long = N_long / (stop - start)
f = open("sequential.csv", "a")
f.write(str(num_cores) + "," + str(throughput_short) + "," + str(throughput_medium) + "," + str(throughput_long) + "\n")
f.close()
print("Sequential:", str(throughput_short), 's')



