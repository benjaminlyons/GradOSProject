#!/bin/bash

# this is a script designed to run and store topcoffea-analysis runs
MAX_RETRIES=75

#source /scratch365/blyons1/miniconda3/etc/profile.d/conda.sh
#conda activate os-project

#module load conda
#conda init
#source /opt/crc/c/conda/miniconda3/4.9.2/etc/profile.d/conda.sh
source ~/miniconda3/etc/profile.d/conda.sh
#source .bashrc
#conda env create -f ~tfisher4/environment.yml
conda activate gradosproj
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

function weak_benchmark(){
	# We can run several weak_benchmarks at once,
	# since they dont interact/interfere at all, once workers are acquired and ready.
	benchmarkfile="wq_weak_benchmarks.py"
	benchmarkmgr="wq-weak-benchmarks-tfisher4"
	# run benchmark in background so we can kick off others
	for workers in 1 2 3 4 5 6 7 8 9 10 15 20 25 30 35 40 45 50 55 60 70 80 90 100; do
		run_benchmark "$benchmarkfile" "$benchmarkmgr-$(printf "%03d" "$workers")" "$workers" > /dev/null &
	done
}

function size_benchmark(){
	# We can run several weak_benchmarks at once,
	# since they dont interact/interfere at all, once workers are acquired and ready.
	benchmarkfile="wq_size_benchmarks.py"
	benchmarkmgr="wq-size-benchmarks-tfisher4"
	# run benchmark in background so we can kick off others
	for workers in 1 2 3 4 5 6 7 8 9 10 15 20 25 30 35 40 45 50 55 60 70 80 90 100; do
		run_benchmark "$benchmarkfile" "$benchmarkmgr-$(printf "%03d" "$workers")" "$workers" > /dev/null &
	done
}

function fs_benchmark(){
	# Don't want to run many fs benchmarks at once, because they all access the same file,
	# so running more benchmarks increases demand for resource outside of that generated by experiment.
	benchmarkfile="wq_fs_benchmarks.py"
	benchmarkmgr="wq-fs-benchmarks-tfisher4"
	movefile="/scratch365/tfisher4/grados-proj/bible%s.txt"
	for workers in 1 2 3 4 5 6 7 8 9 10 15 20 25 30 35 40 45 50 55 60 70 80 90 100; do
		cp $(printf $movefile 1) $(printf $movefile 2)
		run_benchmark "$benchmarkfile" "$benchmarkmgr" "$workers" > /dev/null
		rm $(printf $movefile 2)
	done
}

function task_benchmark_run(){
	benchmarkfile="$1"
	benchmarkmgr="$2"
	workers="$3"

	JOB_NUM=$(condor_submit_workers --cores 4 --memory 16000 --disk 16000 --manager-name "$benchmarkmgr" "$workers" | awk '{print $NF}'| tail -1 | tr -d '.')
	echo "job $JOB_NUM"
	check_workers_ready "$workers" "$JOB_NUM"
	#JOB_NUM=$(wq_submit_workers $workers 4 16000 16000)
	echo "workers ready"
	sleep 10
	echo "starting benchmark"
	python "$benchmarkfile" "$workers"
	#python dask_task_benchmarks.py $workers
	condor_rm "$JOB_NUM"
}

function run_all(){
	weak_benchmark &
	size_benchmark & 
	fs_benchmark &
	wait # wait for all benchmarks to complete
}
