#!/bin/sh
set -e

# type in terminal if permission denied
# chmod +x run_pipeline.sh

source venv/bin/activate
python scripts/0_data_unification.py
python scripts/1_data_profiling.py
python scripts/2_data_validation.py
python scripts/3_data_cleaning.py
python scripts/4_transformation.py
python scripts/5_hash.py
