###################################
# imports
###################################

from polars import col, concat, lit, when
import polars as pl


import sys; sys.path.append('.'); 
from scripts.hdb_helpers import fetch_hdb_data


# import sys
# sys.path.append('/Users/murftech/Dropbox/Datarepo/macroecons/modules/hdb_assess')

import sys
sys.path.append('scripts')
from hdb_helpers import fetch_hdb_data


# VALIDATE = on/off
# VALIDATE = 'off'
VALIDATE = 'off'

###################################
# load data
###################################

'''
REQUIREMENT: Combine the datasets into a single master dataset. The combined data dataset should
contain all the attributes found in all files.
'''

'''
Information from https://data.gov.sg/collections/189/view
# Resale Flat Prices (Based on Approval Date), 1990 - 1999
# d_ebc5ab87086db484f88045b47411ebc5

# Resale Flat Prices (Based on Approval Date), 2000 - Feb 2012
# d_43f493c6c50d54243cc1eab0df142d6a

# Resale Flat Prices (Based on Registration Date), From Mar 2012 to Dec 2014
# d_2d5ff9ea31397b66239f245f57751537

# Resale Flat Prices (Based on Registration Date), From Jan 2015 to Dec 2016
# d_ea9ed51da2787afaf8e51f827c304208
'''


dataset_keys = {
    '1990-1999': 'd_ebc5ab87086db484f88045b47411ebc5',
    '2000-Feb2012': 'd_43f493c6c50d54243cc1eab0df142d6a',  
    'Mar2012-2014': 'd_2d5ff9ea31397b66239f245f57751537',
    '2015-2016': 'd_ea9ed51da2787afaf8e51f827c304208',
}

# # ════════════════════ SCRATCH / PROOF — UNCOMMENT TO VALIDATE, TURN OFF (RE-COMMENT IT) TO RUN PER FUNCTION ═══
if VALIDATE == 'on':
    try:
        # proof: in simple smaller function of direct download, attempts = 1, api call times out on the 3rd back-to-back call always.
        fetch_hdb_data(dataset_keys['2000-Feb2012'], max_attempts=1)
        fetch_hdb_data(dataset_keys['Mar2012-2014'], max_attempts=1)
        fetch_hdb_data(dataset_keys['2015-2016'], max_attempts=1)
    except Exception as e:
        print(repr(e))
    # resolution: alternated to allowing poll interval and multiple attempts in fetch_hdb_data
# # ══════════════════════════════════════════════════════════════════════════════════════════════════════════════

hdbdata1 = fetch_hdb_data(dataset_keys['2000-Feb2012'], max_attempts=5).with_columns(tabling_version = 2)
hdbdata2 = fetch_hdb_data(dataset_keys['Mar2012-2014'], max_attempts=5).with_columns(tabling_version = 3)
hdbdata3 = fetch_hdb_data(dataset_keys['2015-2016'], max_attempts=5).with_columns(tabling_version = 4)


# # ════════════════════ SCRATCH / PROOF — UNCOMMENT TO VALIDATE, TURN OFF (RE-COMMENT IT) TO RUN PER FUNCTION ═══
if VALIDATE == 'on':
    # proof: three data tables cannot be simply joined. With reason: They did not have the same number of rows.

    try:
        pl.concat([hdbdata1, hdbdata2, hdbdata3])
    except Exception as e:
        print(repr(e))
        # ShapeError('unable to append to a DataFrame of width 11 with a DataFrame of width 12')
    hdbdata1.shape #11
    hdbdata2.shape #11
    hdbdata3.shape #12 culprit

    try:
        # how=diagonal: Finds a union between the column schemas and fills missing column values with null.
        bind_relaxed_diagonal = pl.concat([hdbdata1, hdbdata2, hdbdata3], how='diagonal')
        bind_relaxed_diagonal.glimpse()
    except Exception as e:
        print(repr(e))
        # success

    # This option implicitly satisfies that the type of all column names match exactly.
    # however, let us proof it.

    all_cols = sorted(set(hdbdata1.columns) | set(hdbdata2.columns) | set(hdbdata3.columns))
    dtype_report = pl.DataFrame({
        'column': all_cols,
        'df1': [str(hdbdata1.schema.get(c)) for c in all_cols],
        'df2': [str(hdbdata2.schema.get(c)) for c in all_cols],
        'df3': [str(hdbdata3.schema.get(c)) for c in all_cols],
    }).with_columns(
        match = (col('df1') == col('df2')) & (col('df2') == col('df3'))
    )

    showall(dtype_report)

    # resolution: we will do ->
    # pl.concat([hdbdata1, hdbdata2, hdbdata3], how='diagonal')
# # ══════════════════════════════════════════════════════════════════════════════════════════════════════════════


hdbdata_stacked = pl.concat([hdbdata1, hdbdata2, hdbdata3], how='diagonal')



###
# Do: Always discover or provide if not available, a row uniqe id IN any records dataset. 
# Reason: For records by records traceability while doing dev work.
# Proceed: We use Table Version and actual row number, to create the decided record_id
# Note: that this can change if source data changes. It is not for persisting into cleaned assets, 
# but an anchor for joins and treaceability during debugging.
###

df = hdbdata_stacked

df = df.with_row_index(name='row_index')

max_width = len(str(df['row_index'].max()))

df_ided = (
    df
    .with_columns(
        record_id = pl.format(
            '{}-{}',
            col('tabling_version').cast(pl.String).str.zfill(3),
            col('row_index').cast(pl.String).str.zfill(max_width),
        )
    )
    .drop('row_index')
)

# validate
df_ided.select('record_id').show(20)


hdbdata_stacked_ided = df_ided

# describe table:
hdbdata_stacked_ided.glimpse()

# # stdout
# Rows: 459007
# Columns: 13
# $ month               <str> '2000-01', '2000-01', '2000-01', '2000-01', '2000-01', '2000-01', '2000-01', '2000-01', '2000-01', '2000-01'
# $ town                <str> 'ANG MO KIO', 'ANG MO KIO', 'ANG MO KIO', 'ANG MO KIO', 'ANG MO KIO', 'ANG MO KIO', 'ANG MO KIO', 'ANG MO KIO', 'ANG MO KIO', 'ANG MO KIO'
# $ flat_type           <str> '3 ROOM', '3 ROOM', '3 ROOM', '3 ROOM', '3 ROOM', '3 ROOM', '3 ROOM', '3 ROOM', '3 ROOM', '3 ROOM'
# $ block               <str> '170', '174', '216', '215', '218', '320', '320', '330', '330', '332'
# $ street_name         <str> 'ANG MO KIO AVE 4', 'ANG MO KIO AVE 4', 'ANG MO KIO AVE 1', 'ANG MO KIO AVE 1', 'ANG MO KIO AVE 1', 'ANG MO KIO AVE 1', 'ANG MO KIO AVE 1', 'ANG MO KIO AVE 1', 'ANG MO KIO AVE 1', 'ANG MO KIO AVE 1'
# $ storey_range        <str> '07 TO 09', '04 TO 06', '07 TO 09', '07 TO 09', '07 TO 09', '04 TO 06', '07 TO 09', '07 TO 09', '04 TO 06', '07 TO 09'
# $ floor_area_sqm      <f64> 69.0, 61.0, 73.0, 73.0, 67.0, 73.0, 73.0, 68.0, 68.0, 82.0
# $ flat_model          <str> 'Improved', 'Improved', 'New Generation', 'New Generation', 'New Generation', 'New Generation', 'New Generation', 'New Generation', 'New Generation', 'New Generation'
# $ lease_commence_date <i64> 1986, 1986, 1976, 1976, 1976, 1977, 1977, 1981, 1981, 1981
# $ resale_price        <f64> 147000.0, 144000.0, 159000.0, 167000.0, 163000.0, 157000.0, 178000.0, 160000.0, 169000.0, 205000.0
# $ tabling_version     <i32> 2, 2, 2, 2, 2, 2, 2, 2, 2, 2
# $ remaining_lease     <i64> null, null, null, null, null, null, null, null, null, null
# $ record_id           <str> '002-000000', '002-000001', '002-000002', '002-000003', '002-000004', '002-000005', '002-000006', '002-000007', '002-000008', '002-000009'

# persist raw ingested asset:
OUTPUT = hdbdata_stacked_ided

OUTPUT.write_parquet('datalake/hdb/raw/hdbdata', partition_by='tabling_version', mkdir=True)
# python3 modules/hdb_assess/0_data_unification.py
# duration to write this script:
# 2hr:23
