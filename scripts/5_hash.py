
from polars import col, concat, lit, when
import polars as pl

import sys; sys.path.append('.'); 
import scripts.hdb_helpers as dc


hdbdata_transformed = pl.read_parquet('datalake/hdb/transformed/hdbdata')


'''
REQUIREMENT: Hash this identifier column using an irreversible hashing algorithm, while preserving its
uniqueness. Explain the hashing algorithm that you adopted in your documentations.
'''

'''
We use SHA256. It is ubiquiously known to be the go to.
'''



import hashlib


# # ════════════════════ SCRATCH  ══════════════════════════════════════════════════════════════════════════════════════════
# Author's note: we could do in in polars itself, but I would prefer to make the call to not do so with reason:
# 1) we dont want only people who know language well to be able to understand the process without looking up the terms
# 2) even if they could, there are too many nests and a polars expert would take more than seconds to confirm what is happening
# 3) list comprehension is a universal python method, much more audience can read it without looking up the terms, 
# and it has less nesting - speeding up understanding
# 4) there is no computational speed difference, nor is there even less typing comparing polars method to list comprehension
####

# hdbdata_id_hashed = (hdbdata_transformed
#                             .with_columns(
#                                 identifier_hash = col('resale_identifier')
#                                 .map_batches(
#                                     lambda s: pl.Series([hashlib.sha256(x.encode()).hexdigest() for x in s]), return_dtype=pl.String

#     ))
# )

# to_hash_list = hdbdata_transformed['resale_identifier'].to_list()
# hashed_list = [hashlib.sha256(x.encode()).hexdigest() for x in to_hash_list]

# hdbdata_id_hashed = (hdbdata_transformed
#                             .with_columns(identifier_hash = pl.Series(hashed_list))
#                             )
# # ════════════════════════════════════════════════════════════════════════════════════════════════════════

############################
#### Compute Hash Lookup ###
############################
to_hash_list = hdbdata_transformed['resale_identifier'].unique().to_list()
# validate
print(to_hash_list[1:10])

# Actual SHA256 HASH computation here
hash_lookup = [(x, hashlib.sha256(x.encode()).hexdigest()) for x in to_hash_list]
# validate
print(hash_lookup[1:10])

df_hash_lookup = (
    pl.DataFrame(hash_lookup, schema=['resale_identifier', 'identifier_hash'], orient='row')
)
# validate
print(df_hash_lookup)


############################
#### Join hash lookup to data ###
############################
hdbdata_transformed_plus_hash = hdbdata_transformed.join(df_hash_lookup, on='resale_identifier', how='left')

dc.unicity(hdbdata_transformed_plus_hash, 'identifier_hash')

# validate: row count should be unchanged by the join, and hash values should match the map_batches version
print(f"row count before join: {hdbdata_transformed.height:,}")
print(f"row count after join:  {hdbdata_transformed_plus_hash.height:,}")

# validate
hdbdata_transformed_plus_hash.select('record_id', 'resale_identifier', 'identifier_hash').show(5)


print(f"n unique identifier: {hdbdata_transformed_plus_hash['resale_identifier'].n_unique():,}")
print(f"n unique identifier_hash: {hdbdata_transformed_plus_hash['identifier_hash'].n_unique():,}")
print(f"total rows: {hdbdata_transformed_plus_hash.height:,}")


dc.showall(hdbdata_transformed_plus_hash, 'closey')

############################
#### Decide dataset ID columns ###
############################
## WE are assuming that if the exploited dataset is hashed dataset. Then we do not want the users to see what the unhashed value is.
## So we make the call to replace the column resale_dentifier as hashed completely
# we also decide to keep the record_id, so that it can be traced back to source  data in quicker time, if users have bug complaints
hdbdata_cleanly = hdbdata_transformed_plus_hash.drop('resale_identifier').rename({'identifier_hash':'resale_identifier'})

hdbdata_cleanly.glimpse()

OUTPUT = hdbdata_cleanly
OUTPUT.glimpse()
OUTPUT.write_parquet('datalake/hdb/hashed/hdbdata', partition_by=['tabling_version', 'month'], mkdir=True)
print('parquet written')


# python3 modules/hdb_assess/3_transformation.py
# duration to write till here script:
# 2hr:15


