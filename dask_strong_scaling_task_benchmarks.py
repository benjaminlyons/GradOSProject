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

client = Client("tcp://10.32.85.31:8790")

# parallel calculations
N_short = 2**14
N_medium = 2**12

#short
start = time.time()
futures = client.map(increment, range(N_short))
result = client.gather(futures)
stop = time.time()
performance_short =  (stop - start)

#medium
start = time.time()
futures = client.map(delayed_increment, range(N_medium))
result = client.gather(futures)
stop = time.time()

#output
f = open("strong_increment.csv", "a")
performance_long =  (stop - start)
f.write(str(num_cores) + "," + str(performance_short) + "," + str(performance_long) + "\n")
f.close()
print("Increment:", str(performance_short), 's')

# tree reduction - sum list
N_short = 2**14
data = range(N_short)

#short
start = time.time()
while len(data) > 1:
        data = [ delayed(add)(a,b) for a, b in zip(data[::2], data[1::2]) ]
print(data[0].compute())
stop = time.time()
performance_short =  (stop - start)

#medium
data = range(N_medium)
start = time.time()
while len(data) > 1:
        data = [ delayed(add)(a,b) for a, b in zip(data[::2], data[1::2]) ]
print(data[0].compute())
stop = time.time()

#output
f = open("strong_sum.csv", "a")
performance_long =  (stop - start)
f.write(str(num_cores) + "," + str(performance_short) + "," + str(performance_long) + "\n")
f.close()
print("Sum list:", str(performance_short), 's')

# some shared data
N_short = 2**14
data = range(N_short)
N_medium = 2**12

#short
start = time.time()
data = client.map(add, data[:-1], data[1:])
data = client.map(add, data[:-1], data[1:])
result = client.gather(data)
stop = time.time()
performance_short =  (stop - start)

#medium
data = range(N_medium)
start = time.time()
data = client.map(delayed_add, data[:-1], data[1:])
data = client.map(delayed_add, data[:-1], data[1:])
result = client.gather(data)
stop = time.time()

#output
f = open("strong_adds.csv", "a")
performance_long =  (stop - start)
f.write(str(num_cores) + "," + str(performance_short)  + "," + str(performance_long) +  "\n")
f.close()
print("Shared Adds:", str(performance_short), 's')

# purely sequential calculations
a = 1
N_short = 100
start = time.time()
for i in range(N_short):
        a = client.submit(increment, a)
a = client.gather(a)
stop = time.time()
performance_short =  (stop - start)

# medium
N_medium = 100
start = time.time()
for i in range(N_medium):
        a = client.submit(delayed_increment, a)
a = client.gather(a)
stop = time.time()
f = open("strong_sequential.csv", "a")
performance_long =  (stop - start)
f.write(str(num_cores) + "," + str(performance_short)  + "," + str(performance_long)+ "\n")
f.close()
print("Sequential:", str(performance_short), 's')



