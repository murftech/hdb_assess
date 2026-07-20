#!/bin/sh
set -e

source venv/bin/activate
jupytext --to notebook --set-kernel python3 scripts/0_data_unification.py -o 0_data_unification.ipynb
jupytext --to notebook --set-kernel python3 scripts/1_data_profiling.py -o 1_data_profiling.ipynb
jupytext --to notebook --set-kernel python3 scripts/2_data_validation.py -o 2_data_validation.ipynb
jupytext --to notebook --set-kernel python3 scripts/3_data_cleaning.py -o 3_data_cleaning.ipynb
jupytext --to notebook --set-kernel python3 scripts/4_transformation.py -o 4_transformation.ipynb
jupytext --to notebook --set-kernel python3 scripts/5_hash.py -o 5_hash.ipynb

