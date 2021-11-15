#!/bin/sh

# this is a script designed to run and store topcoffea-analysis runs
MAX_RETRIES=75

source /scratch365/blyons1/miniconda3/etc/profile.d/conda.sh
conda activate os-project

function dask_scheduler_start(){
	dask-scheduler &> scheduler.log &
}

# first argument is number of workers to wait for
# second argument is job number
function check_workers_ready(){
	COUNT=0
	sleep 1
	# NUM=$(condor_q | grep "blyons1.*running" | awk '{print $13;}')
	NUM=$(condor_q | grep $2 | awk '{print $7;}')

	if [ "$NUM" == "_" ]; then
		NUM=0
	fi

	while [[ "$NUM" -lt "$1" && "$COUNT" -lt "$MAX_RETRIES" ]];
	do
		((COUNT=$COUNT+1))
		# echo "$NUM/$1 workers ready. Going to sleep"
		sleep 10
		NUM=$(condor_q | grep "$2" | awk '{print $7;}') ;
		if [ "$NUM" == "_" ]; then
			NUM=0
		fi
	done

# 	if [ "$NUM" -eq "$1" ]; then
# 		# echo "All workers ready!"
# 		# return 0
# 	else
# 		# echo " Exceeded max retries for workers "
# 		return 1
# 	fi
}

# this function requests and sets up condor workers
# with dask workers
# first argument is number of workers
# second is number of cpus
# third is memory, fourth is disk
function dask_submit_workers(){
	cat > condor_dask_submit_file <<EOF
universe = vanilla
executable = /scratch365/blyons1/grados-project/dask_submit.sh
arguments = tcp://10.32.85.31:8790
should_transfer_files = yes
when_to_transfer_output = on_exit
error = workers/worker.\$(Process).error
output = workers/worker.\$(Process).output
request_cpus = $2
request_memory = $3
request_disk = $4
+JobMaxSuspendTime = 0
queue $1
EOF
	JOB_NUM=$(condor_submit condor_dask_submit_file | grep -Eo "[0-9]+\." | grep -Eo "[0-9]+")
	check_workers_ready $1 $JOB_NUM
	echo $JOB_NUM
}

function task_benchmark_run(){
	for workers in {1 2 3 4 5 6 7 8 9 10 15 20 25 30 35 40 45 50 55 60 70 80 90 100}; do
		JOB_NUM=$(dask_submit_workers $workers 4 16000 16000)
		sleep 10
		python dask_task_benchmarks.py $workers
		condor_rm $JOB_NUM
	done
}

function task_benchmark_run_workers(){
	workers=$1
	JOB_NUM=$(dask_submit_workers $workers 4 16000 16000)
	sleep 10
	python dask_task_benchmarks.py $workers
	condor_rm $JOB_NUM
}

function task_strong_benchmark_run(){
	for workers in {10..100..10}; do
		JOB_NUM=$(dask_submit_workers $workers 4 16000 16000)
		sleep 10
		python dask_strong_scaling_task_benchmarks.py $workers
		condor_rm $JOB_NUM
	done
}

function task_strong_benchmark_run_workers(){
	workers=$1
	JOB_NUM=$(dask_submit_workers $workers 4 16000 16000)
	sleep 10
	python dask_strong_scaling_task_benchmarks.py $workers
	condor_rm $JOB_NUM
}
