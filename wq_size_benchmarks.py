import time
import sys
import work_queue as wq
import wq_utils
import math

num_machines = int(sys.argv[1])
print("machines:", num_machines)
num_cores = 4*num_machines # each machine has 4 cores
print("cores:", num_cores)

q = wq.WorkQueue(0, name=f'wq-size-benchmarks-tfisher4-{num_machines:03}')
def func(l):
	import time
	time.sleep(0.1)
	return sum(l)

# define small list size
N_small = 25
N_medium = 4000 
N_large = 25000
sizes = [N_small, N_medium, N_large]
data = [list(range(size)) for size in sizes]

num_tasks_small = 200*num_cores
num_tasks_medium = 10*num_cores
num_tasks_large = 4*num_cores
nums_of_tasks = [num_tasks_small, num_tasks_medium, num_tasks_large]
throughputs = []
for size, datum, num_tasks in zip(sizes, data, nums_of_tasks):
	start = time.time()
	for i in range(num_tasks):
		datum[0] = i
		task = wq_utils.give_task_specs(wq.PythonTask(func, datum))
		q.submit(task)
	while not q.empty():
		q.wait()
		print("finished a task")
	stop = time.time()
	throughput = num_tasks / (stop - start)
	throughputs.append(throughput)

f = open("res/wq-size-{num_machines:03}.csv", "a")
result = str(num_cores) + ',' + ','.join(map(str, throughputs)) + '\n'
print(result)

f.write(result)
f.close()

