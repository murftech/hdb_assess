
# %%
###########
## imports
###########
from polars import col, concat, lit, when
import polars as pl

import pandera.polars as pa


import sys; sys.path.append('.'); 
import scripts.hdb_helpers as dc


# Run switches, to be used later for demo ctrl + f: demo

TEST_POISON = 'on'
# TEST_POISON = 'off'
POISONED_SEED = 22345

# %%
#########
## DATA LOAD
#########
hdbdata = pl.read_parquet('datalake/hdb/raw/hdbdata')
hdbdata.glimpse()

# %%
'''
REQUIREMENT: Design data validation rules to validate the following fields (i.e. Date, Town, Flat Type, Flat
Model, storey_range) based on the statistical properties of this master dataset.
'''

# %%
'''
Here are context driven validations:
Followup from: output/hdb_ydataprofile_report_downstream.docx

# All Columns
# Downstream validation rule: Except for lease_remaining , let all columns be subjected to [Null not allowed] during validation.

# flat_type
# Downstream validation rule: Let these be the only values allowed during validation.

Followup from: decision in code
# storey_range
# Downstream validation: Let storey_range be only allowed input format as such eg '28 TO 30'.

# month: 
# Downstream validation: Let month be only allowed input format 'YYYY-MM'.

'''

# %%
# Please note that this list should have come from and be validated by the data author. 
# However, we demo the validation process, by assuming all the distinct values in our data form the valid universe that the data author has defined. 
# in operational run, replace the three lines to pull from dictionary given by data author.
valid_member_towns = hdbdata['town'].unique().sort().to_list() # just for demo
valid_member_flat_types = hdbdata['flat_type'].unique().sort().to_list() # follow up from profiling
valid_member_flat_models = hdbdata['flat_model'].unique().sort().to_list() # just for demo

#######

# %%
pandera_validation_schema = pa.DataFrameSchema(
    {

        'month': pa.Column(str, pa.Check.str_matches(r'^\d{4}-(0[1-9]|1[0-2])$'), nullable=False),  # follow up from profiling

        'town': pa.Column(str, pa.Check.isin(valid_member_towns), nullable=False),  # just for demo
        'flat_type': pa.Column(str, pa.Check.isin(valid_member_flat_types), nullable=False),  # follow up from profiling
        'flat_model': pa.Column(str, pa.Check.isin(valid_member_flat_models), nullable=False), # just for demo

        'storey_range': pa.Column(str, pa.Check.str_matches(r'^\d{2} TO \d{2}$'), nullable=False), # follow up from profiling
    },
    strict=False,
)

# %%
# # ════════════════════ POISONED DEMO CREATION ====================═══
# Create a poisoned dataset containing rows with invalid values to showcase 
# the validation outputs and the validation step works
# We choose five random rows in dataset, for each column, choose 1 non replaced row to 
# make invalid with the value i give below.
# Decide to showcase the persistence of assets, in failed parquet, 
# so we will complete the pipeline as if data is really poisoned, by default.
# turn TEST_POISON to TEST_POISON='off', for the pipeline to compelte using original dataset.

# %%
if TEST_POISON == 'on':
    ### use these errorneous values ###
    poisoned_bad_values = {
        'month': '2025 January',
        'town': 'Taman Mount Austin',
        'flat_type': 'Maisonette',
        'flat_model': 'Model-a',
        'storey_range': 'Higher Floor',
    }

# %%
if TEST_POISON == 'on':
    poisoned_record_ids = (
        hdbdata
        .select('record_id')
        .sample(n=len(poisoned_bad_values), seed=POISONED_SEED)
        ['record_id']
        .to_list()
    )

    print(f'Demo: proceed to poison {len(poisoned_bad_values)} rows, one field each -> '
        f'{dict(zip(poisoned_record_ids, poisoned_bad_values.keys()))}')

# %%
if TEST_POISON == 'on':
    poisoned_hdbdata = hdbdata
    for poisoned_record_id, (poisoned_column, bad_value) in zip(poisoned_record_ids, poisoned_bad_values.items()):
        poisoned_hdbdata = poisoned_hdbdata.with_columns(
            when(col('record_id') == poisoned_record_id)
            .then(lit(bad_value))
            .otherwise(col(poisoned_column))
            .alias(poisoned_column)
        )

    ### test validation_schema on poisoned dataset 

    target_data = poisoned_hdbdata

# # ════════════════════════════════════════ POISONED DEMO CREATION END ══════════════════════════════════════════════════════════════════════

# %%
try:
    target_data = poisoned_hdbdata
    print('validaton target is poisoned data')
except:
    target_data = hdbdata
    print('validaton target is real data')
    



target_data = target_data.with_row_index(name='validate_row_index')

# %%
try:
    pandera_validation_schema.validate(target_data, lazy=True)
    failed_row_indices=[]
    print('no rows failed')
except pa.errors.SchemaErrors as err:
    view_pandera_failure_cases = err.failure_cases 
    # print(str(err))
    failed_row_indices = view_pandera_failure_cases['index'].unique().to_list()
    print(f'Validation: {view_pandera_failure_cases.height} field-level failures, across {len(failed_row_indices)} distinct rows')
    print(view_pandera_failure_cases)

# %%
# act on the validation:
target_validation_failed = (
    target_data
    .filter(col('validate_row_index').is_in(failed_row_indices))
    .drop('validate_row_index')
)
target_validation_failed.glimpse()


target_validation_passed = (
    target_data
    .filter(~col('validate_row_index').is_in(failed_row_indices))
    .drop('validate_row_index')
)


print(f'kept: {target_validation_passed.height:,} rows')
print(f'discarded to failed (validation failures): {target_validation_failed.height:,} rows')


# %%
#################
### DATA WRITE to Datalake
#################
OUTPUT = target_validation_failed
OUTPUT.glimpse()
OUTPUT.write_parquet('datalake/hdb/failed/hdbdata_validate_discard', partition_by='tabling_version', mkdir=True)
print('written parquet')


# %%
OUTPUT = target_validation_passed
OUTPUT.glimpse()
OUTPUT.write_parquet('datalake/hdb/datastore/hdbdata_validate_pass', partition_by='tabling_version', mkdir=True)
print('written parquet')
