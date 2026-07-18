from polars import col, concat, lit, when
import polars as pl

import sys; sys.path.append('.'); 
import scripts.hdb_helpers as dc



TEST_POISON = 'on'
# TEST_POISON = 'off'


hdbdata = pl.read_parquet('datalake/hdb/raw/hdbdata')

# Updated snapshot PROFILING done on date: #2026-07-17

'''
REQUIREMENT:  Perform Data PROFILING on the dataset. You may code your own PROFILING rules or leverage on
open-source data quality frameworks.
'''

'''
We do first these three entry time PROFILING which might be better done self coded rather than packaged frameworoks and fuller summaries.
row counts -> column name and column types -> time variable
'''

####################################
# PROFILING: row count
####################################

# Q: How much row counts we are looking at.
row_count = (hdbdata
             .group_by('tabling_version')
             .len()
             .sort('tabling_version')
)
print(row_count)
# conclude: nothing alarming

####################################
# PROFILING: column names and column types
####################################

# Q: What column names and what column types are we looking at?
columnDescribe = pl.DataFrame({
    'column': hdbdata.columns,
    'dtype': [str(dt) for dt in hdbdata.dtypes],
}).sort('dtype')

columnDescribe.show(100)
# conclude good: column names are all in snake case. 
# Consistent and good formatting helps ease of programming. no change required

# conclude: We only have 11 columns (excluding record_id and tabling_version) — 
# this is a narrow dataset, not a wide one with 20-50+ attributes)

# conclude: The only column name which potentially represents the time attribute for the dataset is [month].
# Note: if there were others, we will catch them in the profiling below.


# For the rest which do not represent the time attribute for the dataset -

# Q: are there any numerics which might be better as factors (String)
# ┌─────────────────────┬─────────┐
# │ column              ┆ dtype   │
# │ ---                 ┆ ---     │
# │ str                 ┆ str     │
# ╞═════════════════════╪═════════╡
# │ floor_area_sqm      ┆ Float64 │
# │ resale_price        ┆ Float64 │
# │ lease_commence_year ┆ Int64   │
# │ remaining_lease     ┆ Int64   │

# conclude: by context of column names, these are sensibly numeric, 
# assigned of the correct set of dtypes. No concerns.

# Q: are there any String type which should be numerical but is parsed as String?
# - regardless of the reason it was so.

# │ town                ┆ String  │
# │ flat_type           ┆ String  │
# │ block               ┆ String  │
# │ street_name         ┆ String  │
# │ storey_range        ┆ String  │
# │ flat_model          ┆ String  │

# conclude: by context of column names, these are sensibly textual, 
# assigned of the correct dtype of string. No concerns.



####################################
### PROFILING: time variable
####################################
# Let us profile the cleanliness of the time variable.
# then let us profile the row count per month

monthvalues = (hdbdata
             .group_by('month', maintain_order=True)
             .len()
)

# Q: What is the format of the time variable
dc.showall(monthvalues)
# conclude: it specifies the month of the record.
# conclude good: the year-month format is in one consistent form - no variances.
# Downstream validation: Let month be only allowed input format 'YYYY-MM'.
# conclude good: The format is immediately sortable into earlier and later, unlike formats like January 2013


# Q: Is there any anomalous row counts in any month?
monthvalues['len'].describe()
# conclude: Average records in a month = 2273
# Max records in a month = 3679, nothing alarmig yet
# Min records in a month: 886, nothing alarming yet
# Note: min was coming not from earliest the month in the records. 
# Hence it is not due to cutoff in early data collection. nothing alarming.
# Nothing alarming overall.


# Q: what is the time span of the dataset:
min_max_month = (hdbdata
               .sort('tabling_version')
               .group_by('tabling_version')
               .agg(
                 pl.min('month').alias('1st_month'),
                 pl.max('month').alias('last_month'),
             )
)

print(min_max_month)
# conclude good: tables 1,2,3 are linked in continuity. No concerns.
# conclude: earliest data is 2000-01 latest data is 2016-12.
# downstream action: will scope it out to only 2012-01 onwards for table 2. In script 2_data_cleaning.


# Q: Are there any gaps in the monthly time series?
data_exists_monthdate = (
    hdbdata
    .select('month').unique()
    .with_columns(monthdate = col('month').str.strptime(pl.Date, '%Y-%m'))
    .sort('monthdate')
    .with_columns(in_data = lit(1))
)


complete_monthdate_list = (
    pl.date_range(
        data_exists_monthdate['monthdate'].min(),
        data_exists_monthdate['monthdate'].max(),
        interval='1mo',
        eager=True,
    )
    .alias('monthdate').to_frame()
)

monthdate_indicator = complete_monthdate_list.join(data_exists_monthdate, on='monthdate', how='left')

dc.showall(monthdate_indicator)
# conclude good: By full visual check, confirm every month is in the datas. Also every month format is correct.


check_12_months_within_year = (
    monthdate_indicator
    .with_columns(year=col('monthdate').dt.year())
    .group_by('year', maintain_order=True)
    .len()
)
dc.showall(check_12_months_within_year)
# conclude good: By more concise check, all years has all and only 12 months, => every month is in the data.

# conclude good: no gaps reported.

print('row counts -> column name and column types -> time variable')
print('In these three specifics, no concerns were yet raised, so there is nothing to triage.')



'''
We now deal with categorical and numerical data profiling
We will now use an external tool which provides more details for lesser work.
We will use the ProfileReport function from the ydata-profiling package to easier and quickly generate the views we need.
'''

'''
# Author note. I  understand the project requests to do work programmatically as much as possible;
# However, i wish to use also ydata_profile to showcase that i am capable in 
# a) leverage [open-source data quality frameworks] and  
# b) integrating out-of-code processes with pipelines as seamlessly as I can.
'''

from ydata_profiling import ProfileReport

profile = ProfileReport(
    hdbdata.to_pandas(),
    title='HDB Resale Master Dataset - Data Profile',
)
profile.to_file('output/hdb_ydataprofile_report.html')

# openfile = 'false'
openfile = 'true'
if openfile == 'true':
    import os
    os.system('open output/hdb_ydataprofile_report.html')

'''
# Becasue the report of this tool is static file based, 
# we need to take screenshots portions from the report and give the profiling decision in-line 
# in a file type like microsoft word docx.
# To review the decisions, please open the docx file if you already have an associated app installed - with the command below:
'''

# openfile = 'false'
openfile = 'true'
if openfile == 'true':
    import os
    os.system('open output/hdb_ydataprofile_report_downstream.docx')


###################################
# Profiling: Numerical Variable
###################################

# categorical variable: 
# Cardinality: does the number of distinct values make sense, thats really all.
# Cardinality 2: Does any of them becasme near to a row key?
# which should be a number or numric time or but is not
# Long Tail
# Format Casing Inconsistency
# Valid Member

'''
Followup from: output/hdb_ydataprofile_report_downstream.docx

# storey_range
# Downstream profiling: We will investigate in code if all values are of this pattern.

# flat_model
# Downstream profiling: We will investigate in code if there are long tail of rare values, 
# which we then ask if they are candidates for merging with other 
'''

# storey_range
# Downstream profiling: We will investigate in code if all values are of this pattern dd TO dd.
hdbdata['storey_range'].value_counts().sort('count').show(1000)
# conclude: Yes. 
# Downstream validation: Let storey_range be only allowed input format as such eg '28 TO 30'.



# flat_model
# Downstream profiling: We will investigate in code if there are long tail of rare values, 
# which we then ask if they are candidates for merging with others

hdbdata['flat_model'].value_counts().sort('count').show(1000)
# ANS: yes there are.

# ANS: Several of these are really variants of the same base design rather than fully independent categories, 
# So that they're natural merge candidates if we look for another grouping with fewer buckets:
# However this is a decision to be made later by stakeholders. 
# Since they are all real HDB flat_model names, there is no need to edit this column.

# ┌────────────────────────┬────────┐
# │ flat_model             ┆ count  │
# │ ---                    ┆ ---    │
# │ str                    ┆ u32    │
# ╞════════════════════════╪════════╡
# │ Premium Apartment Loft ┆ 5      │ Potentially a rare value
# │ 2-room                 ┆ 17     │ Potentially a rare value
# │ Type S2                ┆ 55     │ Potentially a rare value
# │ Improved-Maisonette    ┆ 56     │ Potentially a rare value
# │ Premium Maisonette     ┆ 72     │ Potentially a rare value
# │ Type S1                ┆ 138    │ 
# │ Multi Generation       ┆ 186    │
# │ DBSS                   ┆ 277    │
# │ Terrace                ┆ 349    │
# │ Model A-Maisonette     ┆ 771    │
# │ Adjoined flat          ┆ 933    │
# │ Model A2               ┆ 8064   │
# │ Maisonette             ┆ 12318  │
# │ Apartment              ┆ 18846  │
# │ Standard               ┆ 20219  │
# │ Premium Apartment      ┆ 26340  │
# │ Simplified             ┆ 27334  │
# │ New Generation         ┆ 87611  │
# │ Improved               ┆ 123696 │
# │ Model A                ┆ 131720 │
# └────────────────────────┴────────┘



# categorical variable: 
# Cardinality: does the number of distinct values make sense, thats really all.
# Cardinality 2: Does any of them becasme near to a row key?
# which should be a number or numric time or but is not
# Long Tail
# Format Casing Inconsistency
# Valid Member

'''
Followup from: output/hdb_ydataprofile_report_downstream.docx

# storey_range
# Downstream profiling: We will investigate in code if all values are of this pattern.

# flat_model
# Downstream profiling: We will investigate in code if there are long tail of rare values, 
# which we then ask if they are candidates for merging with other 
'''
