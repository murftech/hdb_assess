# SAMPLED = 'on'
SAMPLED = None

from polars import col, concat, lit, when
import polars as pl

import sys; sys.path.append('.'); 
import scripts.hdb_helpers as dc
from scripts.hdb_helpers import sample_hdb

# import inspect
# print(inspect.getsource(sample_hdb))

dataload = pl.read_parquet('datalake/hdb/cleaned/hdbdata')




#### SAMPLING FOR FIRST EASE OF Validaton during dev ###
if SAMPLED == 'on': 
    hdbdata_v1 = sample_hdb(dataload, N_ROWS=15, SAMPLE_SEED=4)
else:
    hdbdata_v1 = dataload

###################################################

hdbdata_v1.glimpse()


'''
REQUIREMENT: Taking the cleaned data from the previous step, create a new column called “Resale
Identifier”
. The “Resale Identifier” is derived from:
- First character is “S”.

- Next 3 digits is the first 3 digits of the block column, after removing any characters. In
the event the block column has less than 3 digits, prepend with the appropriate number
of zeros in front. (E.g. if block number is “19”, digits will be “019”)

- Next 2 digits is derived by: Taking the 1st and 2nd digit of the average resale price, group
by year-month, town and flat_type. For instance, if the average Ang Mo Kio, 2 Room
Flats in Jan 2017 is $230000, the 2 digits is “23”.

- The last two digits is the month of the current entry (e.g. if the month is 2012-01, the
last two digits is 01).

- The final character is the first character of the town. E.g. Ang Mo Kio, the character is
“A”.
'''


hdbdata_v2_ided = (
    hdbdata_v1
    .with_columns(
        avg_price_group = col('resale_price').mean().over(['month', 'town', 'flat_type']).round(0).cast(pl.Int64)
    )
    .with_columns(
        block_digits = col('block').str.replace_all(r'[^0-9]', '').str.zfill(3).str.slice(0, 3),
        avg_price_digits = col('avg_price_group').cast(pl.String).str.slice(0, 2),
        month_digits = col('month').str.slice(5, 2),
        town_initial = col('town').str.slice(0, 1),
    )
    .with_columns(
        resale_identifier_validate = pl.format('S-{}-{}-{}-{}', col('block_digits'), col('avg_price_digits'), col('month_digits'), col('town_initial')),
        resale_identifier = pl.format('S{}{}{}{}', col('block_digits'), col('avg_price_digits'), col('month_digits'), col('town_initial')),
    )
)

# validate

# validate
validate = (hdbdata_v2_ided
            .select('record_id', 'month', 'town', 'flat_type', 'block', 'resale_price',
                    'avg_price_group', 'block_digits', 'avg_price_digits', 'month_digits', 'town_initial',
                    'resale_identifier_validate', 'resale_identifier')
            .sort('month', 'town', 'flat_type'
            )
)

# sample one random (month, town, flat_type) group to manually spot-check the avg_price_group math
VALIDATE_GROUP_SEED = 4

random_group = (
    validate
    .select('month', 'town', 'flat_type')
    .unique(maintain_order=True)
    .sample(n=1, seed=None)
)
print(random_group)

validate_group = validate.join(random_group, on=['month', 'town', 'flat_type'], how='inner')
dc.showall(validate_group)

z_identifier = hdbdata_v2_ided.select('record_id', 'resale_identifier')




# task: below do cleaner
# v2 v3 whatevr


'''
REQUIREMENT: If there are any duplicate records, take the higher price and discard the lower price one.
'''

'''
A note about the resale identifier, it retains only unique information from 
year-month, town and flat_type and block number.
it excludes informaton from: street_name, storey_range, and lease_commence_year.

Hence, implementing the dedup on identifier is a lossier key than the earlier composite-key.
It intends to take one record as all of the storey_range of the block 
maybe taking the each block's highest sale price that month as the 'Y value'

# actually i am doubtful about why do this at all. the id is lossy.
# could it be that it is a repeat information of the above.

Resolution: Assumed that this is not a repeat sentence to be applied to composite key, and the 
ask is to crete a dataset with 1 resale_identifier as 1 record. 
'''


# hdbdata_id_keyed = (
#     hdbdata_id
#     .with_columns(max_price_in_id = col('resale_price').max().over('resale_identifier'))
# )
#
# hdbdata_id_dedup = (
#     hdbdata_id_keyed
#     .filter(col('resale_price') == col('max_price_in_id'))
#     .drop('max_price_in_id')
# )
#
# failed_id_duplicates = (
#     hdbdata_id_keyed
#     .join(hdbdata_id_dedup, on='record_id', how='anti')
#     .drop('max_price_in_id')
# )

# sort by resale_price descending, then record_id ascending as a deterministic
# tiebreak, then keep only the first row encountered per identifier - guarantees
# exactly one row survives per identifier, even on a genuine price tie.


z_identifier_plus_price = z_identifier.join(hdbdata_v1.select('record_id', 'resale_price'), on='record_id', how='left')
dc.unicity(z_identifier_plus_price, 'resale_identifier')
# failed

z_identifier_dedup = (
    z_identifier_plus_price
    .sort(['resale_price', 'record_id'], descending=[True, False])
    .unique(subset='resale_identifier', keep='first', maintain_order=True)
    .drop('resale_price')
)
dc.dfratio(z_identifier_plus_price, z_identifier_dedup)
# 26 percent dropped

dc.unicity(z_identifier_dedup, 'resale_identifier')
# passed

# hdbdata_dedup = hdbdata.join(z_identifier_dedup, on='record_id', how='right')

# filter hdbdata_v1 to only dedup records, attaching the resale_identifier value in z_identifier
hdbdata_v3_deduped = hdbdata_v1.join(z_identifier_dedup, on='record_id', how='right')


hdbdata_v3_failed_resale_identifier = (
    hdbdata_v1
    .join(hdbdata_v3_deduped, on='record_id', how='anti')
)

print(f"kept: {hdbdata_v3_deduped.shape[0]:,} rows")
print(f"discarded to failed (lower-priced identifier duplicates): {hdbdata_v3_failed_resale_identifier.shape[0]:,} rows")




if SAMPLED == 'on':
    print('break')
else:
    OUTPUT = hdbdata_v3_failed_resale_identifier
    OUTPUT.write_parquet('datalake/hdb/failed/hdbdata_duplicate_resale_identifier_discard', partition_by=['tabling_version'], mkdir=True)

    OUTPUT = hdbdata_v3_deduped
    OUTPUT.glimpse()
    OUTPUT.write_parquet('datalake/hdb/transformed/hdbdata', partition_by=['tabling_version', 'month'], mkdir=True)
    print('written parquet')



