#!/bin/bash

# this is a script designed to run and store topcoffea-analysis runs
MAX_RETRIES=75

#source /scratch365/blyons1/miniconda3/etc/profile.d/conda.sh
#conda activate os-project

#module load conda
#conda init
source /opt/crc/c/conda/miniconda3/4.9.2/etc/profile.d/conda.sh
#source .bashrc
#conda env create -f ~tfisher4/environment.yml
conda activate grad-os-proj
#function dask_scheduler_start(){
#	dask-scheduler &> scheduler.log &
#}

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

 	if [ "$NUM" -eq "$1" ]; then
 		echo "All workers ready!"
 		#return 0
 	else
 		echo "Exceeded max retries for workers "
 		#return 1
 	fi
}

# this function requests and sets up condor workers
# with dask workers
# first argument is number of workers
# second is number of cpus
# third is memory, fourth is disk
function wq_submit_workers(){
	cat > condor_wq_submit_file <<EOF
universe = vanilla
executable = /afs/crc.nd.edu/user/t/tfisher4/wq_submit.sh
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
#arguments = tcp://10.32.85.47:8790
	JOB_NUM=$(condor_submit condor_wq_submit_file | grep -Eo "[0-9]+\." | grep -Eo "[0-9]+")
	check_workers_ready $1 $JOB_NUM
	echo $JOB_NUM
}

function task_benchmark_run(){
	for workers in {10..100..10}; do
        	JOB_NUM=$(condor_submit_workers --cores 4 --memory 16000 --disk 16000 --manager-name wqbenchmark-tfisher4 "$workers" | awk '{print $NF}'| tail -1 | tr -d '.')
		echo "job $JOB_NUM"
        	check_workers_ready "$workers" "$JOB_NUM"
		#JOB_NUM=$(wq_submit_workers $workers 4 16000 16000)
		echo "workers ready"
		sleep 10
		echo "starting benchmark"
        	python workqueue_task_benchmarks.py $workers
		#python dask_task_benchmarks.py $workers
		condor_rm $JOB_NUM
	done
}

task_benchmark_run