#!/bin/bash

# This script should setup the environment and then start the dask-worker
source /scratch365/blyons1/miniconda3/etc/profile.d/conda.sh
conda activate os-project

MANAGER=$1
echo $MANAGER
dask-worker --nthreads 1 --nprocs 4 --memory-limit 4GB $MANAGER
# dask-worker --nthreads 1 --nprocs 4 --memory-limit 4GB $MANAGER
# dask-worker $MANAGER
