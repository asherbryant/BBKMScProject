import pandas as pd
from dask import dataframe as dd


def convert_tsv_to_parquet(file, file_name, out_directory):
    dd = dd.read_csv(file, delimiter="\t")
    dd.to_parquet(f'{out_directory}/{file_name}')
    

def split_interval(file, out_directory):
    interval_dd = dd.read_parquet(file)

    tfs = interval_dd['tfTitle'].unique().compute()
    tfs_list = tfs.to_list()
    tfs_list = list(filter(None, tfs_list))
    tfs_list = [tf.replace('SS18/SSX1', 'SSX1') for tf in tfs_list]
    list_len = len(tfs_list)

    print(f'Spilting GTRD interval file into {list_len} separate TFs files...')

    count = 0
    for tf in tfs_list:
        file_name = tf
        file_location = f'{out_directory}/{file_name}.parquet'
        tf_df = interval_dd.groupby('tfTitle').get_group(tf).compute()
        tf_df.to_parquet(file_location)
        count += 1    
        print(str(count) + ". " + tf + " done...")
        
    return(f'GTRD interval file split into {count} separate files.')
