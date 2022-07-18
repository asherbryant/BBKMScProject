
import pandas as pd
import glob
from dask import dataframe as dd

def create_tfbs(in_directory, out_directory, number_of_bs):
    tf_files = glob.glob(in_directory + "/*.parquet", recursive=True)
    for i in range(len(tf_files)):
        tf_name = tf_files[i][len(in_directory)+1:-8]
        df = pd.read_parquet(tf_files[i])
        if df.shape[0] >= number_of_bs:
            df = df.sort_values('peak.count',ascending = False).head(number_of_bs)
            df['center'] = df['START'] + df['summit']
            df = df[['id','#CHROM', 'START', 'END', 'center', 'summit']].set_index('id')
            df.to_parquet(f'{out_directory}/{tf_name}_{number_of_bs}.parquet')
            print(f'{tf_name} binding site file created.')
        else:
            print(f'{tf_name} does not meet the binding site threshold.')
    return "All TF files processed."


# may be should add column as a parameter in case file format changes in future or user wants to group by something else
def split_interval(file, out_directory):
    interval_dd = dd.read_parquet(file)

    tfs = interval_dd['tfTitle'].unique().compute()
    tfs_list = tfs.to_list()
    tfs_list = list(filter(None, tfs_list))
    tfs_list = [''.join(filter(str.isalnum, tf)) for tf in tfs_list]
    list_len = len(tfs_list)

    print(f'Spilting interval file into {list_len} separate TFs files...')

    count = 0
    for tf in tfs_list:
        file_name = tf
        file_location = f'{out_directory}/{file_name}.parquet'
        tf_df = interval_dd.groupby('tfTitle').get_group(tf).compute()
        tf_df.to_parquet(file_location)
        count += 1    
        print(str(count) + ". " + tf + " done...")
        
    return(f'Splitting of interval file into {count} separate files complete.')
