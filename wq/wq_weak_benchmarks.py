import time
import sys
import work_queue as wq
import math
import random
import json
import wq_utils
import os

num_workers = int(sys.argv[1])
out_dir = sys.argv[2]
num_cores = num_workers * 4
print("cores:", num_cores)

#with open(sys.argv[2]) as problem_sizes_file:
#	problem_sizes = json.loads(problem_sizes.read())

# WORKQUEUE SETUP
q = wq.WorkQueue(0, name=f'wq-weak-benchmarks-tfisher4-{num_workers:03}')

# inspired in part by https://matthewrocklin.com/blog/work/2017/07/03/scaling
def add(a, b):
	return a + b

def increment(x):
	return x + 1

def delayed_add(a,b):
	import time
	time.sleep(0.1)
	return a + b

def delayed_increment(x):
	import time
	time.sleep(0.1)
	return x + 1

def long_add(a,b):
	import time
	time.sleep(1)
	return a + b

def long_increment(x):
	import time
	time.sleep(1)
	return x + 1

increments = [increment, delayed_increment, long_increment]
adds = [add, delayed_add, long_add]

# NOTE: concern: as stated in DASK benchmarking blog, since these functions are all fast, adding more cores likely won't help
# ===== Parallel: Independent Increments =====
pll_N_short = 2**8*num_cores
pll_N_medium = 2**7*num_cores
pll_N_long = 4*num_cores
pll_sizes = [pll_N_short, pll_N_medium, pll_N_long]

#pll_sizes = problem_sizes['parallel']
pll_fxns = increments
pll_throughputs = []
for size, fxn in zip(pll_sizes, pll_fxns):
	start = time.time()
	for i in range(size):
		task = wq_utils.give_task_specs(wq.PythonTask(fxn, i))
		q.submit(task)
	while not q.empty():
		t = q.wait() # block indefinitely: not doing other work in the meantime
	    #print(f'task {t.id} completed with result {t.output}')    
	stop = time.time()
	pll_throughputs.append(size / (stop - start))
f = open(os.path.join(out_dir, f"wq-parallel-{num_workers:03}.csv"), "a")
f.write(str(num_cores) + ',' + ','.join(map(str, pll_throughputs)) + '\n')
f.close()
print("Parallel: ", ' | '.join(map(str, pll_throughputs)))


# ===== Tree Reduction: Sum List =====
class ReduceNode:
	def __init__(self):
		self.result = None
		self.sibling = None

def build_reduce_tree(size):
	lvl = [ReduceNode() for i in range(size//2)]
	tree = [lvl]
	while len(lvl) > 1:
		nxt = [ReduceNode() for i in range(len(lvl)//2)]
		# set parents and siblings of last level
		for even in range(0, len(lvl), 2):
			#lvl[even].parent = lvl[even+1].parent = nxt[even//2]
			lvl[even].sibling = lvl[even+1]
			lvl[even+1].sibling = lvl[even]
		tree.append(nxt)
		lvl = nxt
	return tree

red_N_short = 2**7 * num_cores
red_N_medium = 2**7 * num_cores
red_N_long = 4 * num_cores
red_sizes = [red_N_short, red_N_medium, red_N_long]

#red_sizes = problem_sizes['reduction']
red_fxns = adds
red_throughputs = []

for size, fxn in zip(red_sizes, red_fxns):
	# this naive tree reduction (also used in dask benchmark)
	# only sums elements up to index which is highest power of 2 smaller than the number
	size = 2 ** math.floor(math.log(size, 2))
	data = iter(range(size))
	reduce_tree = build_reduce_tree(size)
	tree_loc = {}
	# start timer
	start = time.time()
	# submit starter tasks
	for even in range(0, size, 2):
		task = wq_utils.give_task_specs(wq.PythonTask(fxn, next(data), next(data)))
		tid = q.submit(task)
		tree_loc[tid] = (0, even//2)
	# submit dependent tasks as dependencies complete
	while not q.empty():
		t = q.wait()
		level, idx = tree_loc[t.id]
		node = reduce_tree[level][idx]
		node.result = t.output
		if node.sibling and node.sibling.result:
			parent_task = wq_utils.give_task_specs(wq.PythonTask(fxn, node.result, node.sibling.result))
			tid = q.submit(parent_task)
			tree_loc[tid] = (level+1, idx//2)
	# save measurements
	stop = time.time()
	throughput = size / (stop - start)
	red_throughputs.append(throughput)
	# verify result (for testing correctness)
	print(
		len(reduce_tree[-1]) == 1,
		reduce_tree[-1][0].result == size * (size - 1) / 2, # (gauss's formula)
		reduce_tree[-1][0].result
	)
# output results to file
f = open(os.path.join(out_dir, f"wq-reduction-{num_workers:03}.csv"), "a")
f.write(str(num_cores) + ',' + ','.join(map(str, red_throughputs)) + '\n')
f.close()
print("Reduction: ", ' | '.join(map(str, red_throughputs)))
	

# ===== Shared Data: Sum Pairs, Twice =====
# 1  2  3  4
# 
# 1  2  3
# 2  3  4
#  \/ \/
shd_N_short = 2**6*num_cores
shd_N_medium = 2**7 * num_cores
shd_N_long = 4 * num_cores
shd_sizes = [shd_N_short, shd_N_medium, shd_N_long]

#shd_sizes = problem_sizes['shared']
shd_fxns = adds
shd_throughputs = []

for size, fxn in zip(shd_sizes, shd_fxns):
	data = range(size)
	int_data = [None for _ in range(size-1)]
	final_data = [None for _ in range(size-1)]
	tid_to_idx = {}
	started = set()

	# Again, no built-in way to handle dependencies, so construct manually
	# Dependency management left to the manager
	#  - tried to make more efficient by kicking off stage 2 tasks as soon as dependencies ready
	start = time.time()
	for i, (a, b) in enumerate(zip(data[:-1], data[1:])):
		task = wq_utils.give_task_specs(wq.PythonTask(fxn, a, b))
		tid_to_idx[q.submit(task)] = i #TODO: need to do some error handling here?
	while not q.empty():
		t = q.wait()
		idx = tid_to_idx[t.id]
		int_data[idx] = t.output
		if idx-1 > 0 and int_data[idx-1] and idx-1 not in started:
			task = wq_utils.give_task_specs(wq.PythonTask(fxn, int_data[idx-1], int_data[idx]))
			tid_to_idx[q.submit(task)] = idx-1
			started.add(idx-1)
		if idx+1 < len(int_data) and int_data[idx+1] and idx not in started:
			task = wq_utils.give_task_specs(wq.PythonTask(fxn, int_data[idx], int_data[idx+1]))
			tid_to_idx[q.submit(task)] = idx
			started.add(idx)
	stop = time.time()
	throughput = size / (stop - start)
	shd_throughputs.append(throughput)

# output results to file
f = open(os.path.join(out_dir, f"wq-shared-{num_workers:03}.csv"), "a")
f.write(str(num_cores) + ',' + ','.join(map(str, shd_throughputs)) + '\n')
f.close()
print("Shared: ", ' | '.join(map(str, shd_throughputs)))


# ===== Sequential: Incrementing a Counter =====
a = 1
seq_N_short = 100
seq_N_medium = 100
seq_N_long = 100
seq_sizes = [seq_N_short, seq_N_medium, seq_N_long]

#seq_sizes = problem_sizes['sequential']
seq_fxns = increments
seq_throughputs = []
for size, fxn in zip(seq_sizes, seq_fxns):
	start = time.time()
	for i in range(size):
		task = wq_utils.give_task_specs(wq.PythonTask(fxn, a))
		q.submit(task)
		a = q.wait().output
	stop = time.time()
	throughput = size / (stop - start)
	seq_throughputs.append(throughput)
f = open(os.path.join(out_dir, f"wq-sequential-{num_workers:03}.csv"), "a")
f.write(str(num_cores) + ',' + ','.join(map(str, seq_throughputs)) + '\n')
f.close()
print("Sequential: ", ' | '.join(map(str, seq_throughputs)))


"""
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
        next_L[tid_to_idx[t.id]] = t.output # TODO: consider failures here?
    L = next_L 
stop = time.time()
f = open("res3/sum.csv", "a")
throughput = N / (stop - start)
f.write(str(num_cores) + "," + str(throughput) + "\n")
f.close()
print("Sum list:", str(throughput), 's')
"""
