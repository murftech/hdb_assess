#!/bin/sh
set -e

# type in terminal if permission denied
# chmod +x run_papermill_pipeline.sh

source venv/bin/activate

papermill 0_data_unification.ipynb 0_data_unification.ipynb --log-output
papermill 1_data_profiling.ipynb 1_data_profiling.ipynb --log-output
papermill 2_data_validation.ipynb 2_data_validation.ipynb --log-output
papermill 2_data_validation.ipynb 2_data_validation.ipynb --log-output
papermill 3_data_cleaning.ipynb 3_data_cleaning.ipynb --log-output
papermill 4_transformation.ipynb 4_transformation.ipynb --log-output
papermill 5_hash.ipynb 5_hash.ipynb -k python3 --log-output
