#!/bin/bash

# activate conda env so we know about the work_queue_worker executable
source /scratch365/tfisher4/grados-proj/miniconda3/etc/profile.d/conda.sh
conda activate /scratch365/tfisher4/grados-proj/miniconda3/envs/worker-env

manager="$1"

# start 4 worker processes on this one worker machine
work_queue_worker --cores 1 --memory 4000 --disk 4000 --manager-name "$manager" &
work_queue_worker --cores 1 --memory 4000 --disk 4000 --manager-name "$manager" &
work_queue_worker --cores 1 --memory 4000 --disk 4000 --manager-name "$manager" &
work_queue_worker --cores 1 --memory 4000 --disk 4000 --manager-name "$manager" &
wait
