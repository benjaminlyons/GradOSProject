#!/bin/bash

# activate conda env so we know about the cctools (condor_submit, all that) executables
source /scratch365/tfisher4/grados-proj/miniconda3/etc/profile.d/conda.sh
conda activate /scratch365/tfisher4/grados-proj/miniconda3/envs/worker-env

mkdir workers
source wq_sim_run.sh

# run specified function ($1), and output to specified data dir ($2)
"$1" "$2" # > /dev/null
wait
