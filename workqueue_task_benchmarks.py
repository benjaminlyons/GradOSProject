import time
import sys
import work_queue as wq
import math
import random

num_workers = int(sys.argv[1])
num_cores = num_workers * 4
print("cores:", num_cores)

# WORKQUEUE SETUP
q = wq.WorkQueue(name='wqbenchmark-tfisher4')

# inspired in part by https://matthewrocklin.com/blog/work/2017/07/03/scaling
def add(a, b):
        return a + b

def increment(x):
        return x + 1

def give_task_specs(task, cores=1, memory=16, disk=16):
	task.specify_cores(cores)
	task.specify_memory(memory)
	task.specify_disk(disk)
	return task


# NOTE: concern: as stated in DASK benchmarking blog, since these functions are all fast, adding more cores likely won't help
# Parallel Calculations
N = 2**10*num_cores
start = time.time()
for i in range(N):
    task = give_task_specs(wq.PythonTask(increment, i))
    q.submit(task)
#tasks = [wq.PythonTask(increment, i).specify_cores(1).specify_memory(16).specify_disk(16) for i in range(N)]
while not q.empty():
    t = q.wait() # block indefinitely: not doing other work in the meantime
    # print(f'task {t.id} completed with result {t.output}')
    
stop = time.time()
f = open("res/increment.csv", "a")
throughput = N / (stop - start)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()
print("Increment:", str(throughput), 's')

# tree reduction - sum list
N = int(5000 * math.sqrt(num_cores)) #NOTE: why sqrt?
N = int(4096 * num_cores) # make it a power of two: num_cores is a multiple of 4
L = [random.randint(0, 10000) for _ in range(N)]
start = time.time()
# WQ cannot express dependencies like dask graph: do some gymnastics here to simulate tree reduce
# NOTE: WQ meant for independent ensemble of problems, not complex dependencies
# build dependency tree?
while len(L) > 1:
    tid_to_idx = {}
    next_L = [0 for _ in range(len(L)//2)]
    for i in range(len(L)//2):
        task = give_task_specs(wq.PythonTask(add, L[2*i], L[2*i+1]))
        tid_to_idx[q.submit(task)] = i #TODO: need to do some error handling here?
    while not q.empty():
        t = q.wait()
        next_L[tid_to_idx[t.taskid]] = t.output # TODO: consider failures here?
    L = next_L 
stop = time.time()
f = open("res/sum.csv", "a")
throughput = N / (stop - start)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()
print("Sum list:", str(throughput), 's')

# some shared data
N = 2**10*num_scores
data = range(2**10*num_cores)
int_data = [None for _ in range(N-1)]
final_data = [None for _ in range(N-1)]
tid_to_idx = {}
started = set()

# Again, no built-in way to handle dependencies, so construct manually
# Dependency management left to the manager
#  - tried to make more efficient by kicking off stage 2 tasks as soon as dependencies ready
start = time.time()
for i, (a, b) in enumerate(zip(data[:-1], data[1:])):
    task = give_task_specs(wq.PythonTask(add, a, b))
    tid_to_idx[q.submit(task)] = i #TODO: need to do some error handling here?
while not q.empty():
    t = q.wait()
    idx = tid_to_idx[t.taskid]
    int_data[idx] = t.output
    if idx-1 > 0 and int_data[idx-1] and idx-1 not in started:
        task = wq.PythonTask(add, int_data[idx-1], int_data[idx]).specify_cores(1).specify_memory(16).specify_disk(16)
        tid_to_idx[q.submit(task)] = idx-1
        started.add(idx-1)
    if idx+1 < len(int_data) and int_data[idx+1] and idx not in started:
        task = wq.PythonTask(add, int_data[idx], int_data[idx+1]).specify_cores(1).specify_memory(16).specify_disk(16)
        tid_to_idx[q.submit(task)] = idx
        started.add(idx)

# 1  2  3  4
# 
# 1  2  3
# 2  3  4
#  \/ \/

"""
for i, (a, b) in enumerate(zip(int_data[:-1], int_data[1:])):
    task = wq.PythonTask(add, a, b).specify_cores(1).specify_memory(16).specify_disk(16)
    tid_to_idx[q.submit(task)] = i #TODO: need to do some error handling here?
while not q.empty():
    t = q.wait()
    final_data[tid_to_idx[t.taskid]] = t.output
"""
stop = time.time()

f = open("res/adds.csv", "a")
throughput = N / (stop - start)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()
print("Shared Adds:", str(throughput), 's')

# purely sequential calculations
a = 1
N = 1000
start = time.time()
for i in range(N):
    task = give_task_specs(wq.PythonTask(increment, a))
    q.submit(task)
    a = q.wait().output
stop = time.time()
f = open("res/sequential.csv", "a")
throughput = N / (stop - start)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()
print("Sequential:", str(throughput), 's')



