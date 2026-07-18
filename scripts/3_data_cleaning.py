# SAMPLED = 'on'
SAMPLED = None

from polars import col, concat, lit, when
import polars as pl


import sys; sys.path.append('.'); 
import scripts.hdb_helpers as dc
from scripts.hdb_helpers import sample_hdb

import inspect
print(inspect.getsource(sample_hdb))


dataload = pl.read_parquet('datalake/hdb/datastore/hdbdata_validate_pass')


'''
REQUIREMENT:
You will need to design
an ETL pipeline using datasets from January 2012 to December 2016.
'''

# SCOPE THE ASSET FROM HERE ON TI ONLY BETWEEN JAN 2012 TO DEC 2016
dataload_periodscope = dataload.filter(col('month')>='2012-01').filter(col('month')<='2016-12')

# validate
dataload_periodscope['month'].describe()


#### SAMPLING FOR FIRST EASE OF DATA CLEANING ###
if SAMPLED == 'on': 
    print('using sampled reduced data')
    hdbdata_v1 = sample_hdb(dataload_periodscope, N_ROWS=15, SAMPLE_SEED=4)
else:
    print('using full data')
    hdbdata_v1 = dataload_periodscope

###################################################

hdbdata_v1.glimpse()


'''
REQUIREMENT: Assume HDB lease is 99 years old, recompute remaining lease as of today. Remaining lease
should be rounded down to Years and Months.
Source: description by DATAGOV,
Remaining Lease	-	Remaining time left on the lease of flat.

'''

dc.sortcount(hdbdata_v1, 'remaining_lease')
# We assume the reason to recompute remaining lease is, that 
# 1) the value does not exist for 2 tabling versions, and we can recompute a valid information for all tabling versions anyway

'''
# compute: remaining lease as of today, rounded down to years + months
# Since the best information we have which is lease_commence_year, does not have month, 
# but we need to have a sensible proxy of Year and Months of lease remaining, 
# we will make assumption for the lease for all flats commences on 1st January of lease_commence_year
'''

hdbdata_v1.select('lease_commence_date').show()
# but it is a year! so we should rename it going forward and in later data assets for clarity
hdbdata_v1 = hdbdata_v1.rename({'lease_commence_date': 'lease_commence_year'})

# WILL DO THIS WHEN I HAVE DONE VALIDATION FROM NUMERICS


from datetime import datetime
TODAY_YEAR = datetime.now().year
TODAY_MONTH = datetime.now().month
print(TODAY_YEAR)
print(TODAY_MONTH)


hdbdata_v1_sel = hdbdata_v1.select('record_id', 'lease_commence_year')

hdbdata_v2 = (
    hdbdata_v1_sel
    .with_columns(lit(TODAY_YEAR).alias('TODAY_YEAR'))
    .with_columns((lit(TODAY_MONTH) + 1).alias('NEXT_MONTH'))
    .with_columns(lease_end_year = col('lease_commence_year') + 99)
    .with_columns(nb_years_left_from_next_year = col('lease_end_year') - (TODAY_YEAR + 1))
    .with_columns(nb_months_to_next_year = 12 - col('NEXT_MONTH'))
)

# initially we decided to do by months, but the logic was not apparent and checkable by external audience quickly.
# Hence we decided to do the below sequence of logic 

# validate
hdbdata_v2.show(1)
hdbdata_v2.show(2)
hdbdata_v2.show(3)


# build the remaining_lease column
z_remaining_lease = (hdbdata_v2
                       .with_columns(remaining_lease = pl.format('{} years, {} months', col('nb_years_left_from_next_year'), col('nb_months_to_next_year')))
                       .select('record_id', 'remaining_lease')
)
z_remaining_lease
# subrename


hdbdata_recompute_remanining_lease = (hdbdata_v1
                                     .drop('remaining_lease')
                                     .join(z_remaining_lease, on='record_id', how='left')
)

# validate
dc.showcol(hdbdata_v1, 'remaining_lease')
dc.showcol(hdbdata_recompute_remanining_lease, 'remaining_lease')

hdbdata_v2 = hdbdata_recompute_remanining_lease


# python3 modules/hdb_assess/0_data_unification.py
# duration to write till here script:
# 2hr:45


'''
REQUIREMENT: You may assume the composite key for the dataset is all columns except the resale price. If
all columns have the exact same value, except for the resale price (i.e. duplicated key), take
the higher price and discard the lower price into the “failed” dataset (if any).

My doubts: We assume if key is duplicated, but price is somehow exactly the same, they are not defined as duplicated key.
Logic can be modified if that was not intended.
'''


# composite key = every column except resale_price, excluding also record_id.

composite_key = [c for c in hdbdata_v2.columns
                       if c not in ('resale_price', 'record_id', 'tabling_version')]

dc.unicity(hdbdata_v2, composite_key)


# # ════════════════════ SCRATCH / PROOF — UNCOMMENT TO VALIDATE, TURN OFF (RE-COMMENT IT) TO RUN PER FUNCTION ═══
# proof: 
# commented out: max().over() + filter allows ties through (if two rows in the same
# composite-key group share the exact same max resale_price, both pass the filter and
# both survive - the group isn't actually deduped down to one row).

hdbdata_v3_keyed = (hdbdata_v2
                 .with_columns(max_resale_price_in_key = col('resale_price').max().over(composite_key))
                 )

hdbdata_v3_deduped = (
    hdbdata_v3_keyed
    .filter(col('resale_price') == col('max_resale_price_in_key'))
    .drop('max_resale_price_in_key')
)

failed_duplicates = (
    hdbdata_v3_keyed
    .join(hdbdata_v3_deduped, on='record_id', how='anti')
    .drop('max_resale_price_in_key')
)

dc.unicity(hdbdata_v3_deduped, composite_key)
# warning: multiplicity violated!
# victims are returned in global as uvictims.
# multiplicity pcnt
# 446540
# shape: (4, 3)
# ┌─────────┬────────┬──────────┐
# │ nb_rows ┆ count  ┆ pcnt     │
# │ ---     ┆ ---    ┆ ---      │
# │ u32     ┆ u32    ┆ f64      │
# ╞═════════╪════════╪══════════╡
# │ 1       ┆ 445822 ┆ 0.998392 │
# │ 2       ┆ 715    ┆ 0.001601 │ Tie in Resale Price
# │ 4       ┆ 2      ┆ 0.000004 │ Tie in Resale Price
# │ 3       ┆ 1      ┆ 0.000002 │ Tie in Resale Price
# └─────────┴────────┴──────────┘
# prove that data is still not unique in composite key
# Resolution: Alternate to tie breaker filter to top 1 row method of taking highest
# # ══════════════════════════════════════════════════════════════════════════════════════════════════════════════


# sort by resale_price descending (highest price first), then record_id ascending
# as a deterministic tiebreak, then keep only the first row encountered per composite-key
# group - this guarantees exactly one row survives per group, even on a genuine price tie.

hdbdata_v3_deduped = (
    hdbdata_v2
    .sort(['resale_price', 'record_id'], descending=[True, False])
    .unique(subset=composite_key, keep='first', maintain_order=True)
)

dc.unicity(hdbdata_v3_deduped, composite_key)
# passed

hdbdata_v3_failed_composite = (
    hdbdata_v2
    .join(hdbdata_v3_deduped, on='record_id', how='anti')
)

print(f"kept: {hdbdata_v3_deduped.shape[0]:,} rows")
print(f"discarded to failed (lower-priced duplicates): {hdbdata_v3_failed_composite.shape[0]:,} rows")

if SAMPLED == 'on':
    print('break')
else:
    OUTPUT = hdbdata_v3_failed_composite
    OUTPUT.write_parquet('datalake/hdb/failed/hdbdata_duplicate_composite_discard', partition_by='tabling_version', mkdir=True)

    OUTPUT = hdbdata_v3_deduped
    OUTPUT.write_parquet('datalake/hdb/cleaned/hdbdata', partition_by=['tabling_version', 'month'], mkdir=True)
    print('written parquet')


# python3 modules/hdb_assess/0_data_unification.py
# durations to write till here script:
# 14min
# 1:08 (for tie breaking and df names cleaning)

