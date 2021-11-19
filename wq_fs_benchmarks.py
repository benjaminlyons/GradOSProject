import time
import work_queue as wq
import wq_utils
import sys

def read_file():
	f = open("/scratch365/tfisher4/grados-project/bible2.txt")
	data = f.read()
	count = len(data)
	f.close()

num_machines = int(sys.argv[1])
print("machines:", num_machines)
num_cores = 4*num_machines # each machine has 4 cores
print("cores:", num_cores)

q = wq.WorkQueue(0, name="wq-fs-benchmarks-tfisher4") # indepdentent of # machines
start = time.time()
for i in range(num_cores):
	task = wq_utils.give_task_specs(wq.PythonTask(read_file))
	q.submit(task)
while not q.empty(): q.wait()
stop = time.time()

f = open("res/wq-filesystem.csv", "a")
throughput = num_cores / (stop - start)
print("Throughput:", throughput)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()

