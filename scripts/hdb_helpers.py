
import polars as pl



def fetch_hdb_data(hdb_dataset_id, max_attempts=10, poll_interval=5):

    import time
    import requests
    import polars as pl


    base_url = "https://api-open.data.gov.sg/v1/public/api/datasets"

    initiate_url = f"{base_url}/{hdb_dataset_id}/initiate-download"
    print('sending')
    print(initiate_url)
    requests.get(
        initiate_url,
        # headers={"Content-Type": "application/json"},
    )

    download_url = ""
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(f"{base_url}/{hdb_dataset_id}/poll-download")

        print("Trying Fetching download URL...")
        try:
            download_url = resp.json().get("data", {}).get("url", "")
        except AttributeError:
            download_url = ""
        # print(download_url)

        if download_url:
            break
        time.sleep(poll_interval)

    if not download_url:
        raise RuntimeError(f"API did not return a download URL after {max_attempts} polling attempts.")

    print("polars Reading data...")
    df = pl.read_csv(
        download_url,
        schema_overrides={"floor_area_sqm": pl.Float64, 
                          "resale_price": pl.Float64, 
                          "block": pl.String}

    )

    latest_data = df.group_by("month").len(name="number_of_sales").sort("month", descending=True)
    print('latest data count')
    latest_data.show(12)
    print(f"Done. Loaded {len(df):,} rows and {len(df.columns)} columns.")

    return df

# tester
# abc = fetch_hdb_data()
# abc.glimpse()


def sample_hdb(hdb_df, N_ROWS, SAMPLE_SEED):
    import polars as pl
    core_cols = ['month', 'town', 'flat_type', 'block', 'street_name', 'storey_range',
                'floor_area_sqm', 'flat_model', 'lease_commence_date', 'resale_price']
    dataload_nonulls = hdb_df.drop_nulls(subset=core_cols)

    SAMPLE_SEED = 1   # change to 2, 3, 4, 5 for different samples
    N_PER_TABLE= round(N_ROWS/3)

    df_sample = (
        dataload_nonulls
        .group_by('tabling_version', maintain_order=True)
        .map_groups(lambda g: g.sample(n=min(N_PER_TABLE, g.height), seed=SAMPLE_SEED))
    )
    return(df_sample)

    hdbdata = df_sample

def showall(dataframe, *modes, tbl_width_chars=250):
    tbl_rows = None if 'closey' in modes else -1
    tbl_cols = None if 'closex' in modes else -1
    with pl.Config(tbl_rows=tbl_rows, tbl_cols=tbl_cols, tbl_width_chars=tbl_width_chars):
        print(dataframe)




# def showall(dataframe, *modes):
#     tbl_rows = None if 'closey' in modes else -1
#     tbl_cols = None if 'closex' in modes else -1
#     with pl.Config(tbl_rows=tbl_rows, tbl_cols=tbl_cols):
#         print(dataframe)



def showcol(df, column):
    print(df.filter(pl.col(column).is_not_null()).select(column))


def sortcount(df, coltuple, truncate=True):
    if isinstance(coltuple, str):
        coltuple = [coltuple]
    factor_table = df.group_by(coltuple).len().rename({'len': 'count'}).sort('count', descending=True)
    table_total = factor_table['count'].sum()
    print(table_total)
    factor_table_pcnt = factor_table.with_columns((pl.col('count') / table_total).alias('pcnt'))
    print(factor_table_pcnt.head(40))
    return factor_table_pcnt


def unicity(dataset, coltuple, showpcnt=True):
    print('victims will be returned in global as uvictims')
    global uvictims
    if isinstance(coltuple, str):
        coltuple = [coltuple]
    unitable = (dataset.group_by(coltuple).len()
                .rename({'len': 'nb_rows'})
                .sort('nb_rows', descending=True))
    multiplicity = unitable.filter(pl.col('nb_rows') > 1)

    if multiplicity.height == 0:
        print("good!: multiplicity passed!:")
        uvictims = None
    else:
        print("warning: multiplicity violated!")
        uvictims = dataset.join(multiplicity.select(coltuple), on=coltuple, how='semi').sort(coltuple)
        print('victims are returned in global as uvictims.')
        if showpcnt:
            print('multiplicity pcnt')
            sortcount(unitable, 'nb_rows', truncate=False)
    print('unicity ended')




def dfratio(df_before, df_after):
    global ratio_delta
    before_count = df_before.height
    after_count = df_after.height
    ratio_delta = after_count / before_count
    print(f'before count: {before_count}')
    print(f'after count: {after_count}')
    print(f'ratiodelta: {ratio_delta:.3f}')
    print('ratio_delta returned as global variable for exception raising')

