import glob
import pandas as pd
from dask import dataframe as dd



def load_high_spec_tfs(input_file):
    with open(input_file, 'r') as file:
        high_spec_tfs = file.read().splitlines()
    
    return high_spec_tfs


def get_tfbs(in_directory, sites=1000, bases=1000, high_spec=True):
    
    tf_files = glob.glob(in_directory + "/*.parquet", recursive=True)
    number_of_files = len(tf_files)
    print(f"Getting TF binding sites for {number_of_files} TFs...")
    
    tf_names = []
    tfbs_df_list = []
    excluded = 0
    for i in range(number_of_files):
        tf_name = tf_files[i][len(in_directory)+1:-8]
        df = pd.read_parquet(tf_files[i])
        df = df[~df['#CHROM'].isin(['chrMT','chrX', 'chrY'])]
        
        
        if high_spec == True:
            high_spec_file = 'high-specificity.txt'
            filtered_list = load_high_spec_tfs(high_spec_file)
        else:
            filtered_list = [tf_name]
        
        
        if df.shape[0] >= sites and tf_name in filtered_list:
            tf_names.append(tf_name)
            df = df.sort_values('peak.count',ascending = False).head(1000)
            df['ROI_start'] = df['START'] + df['summit'] - bases
            df = df[['#CHROM', 'ROI_start']].set_index('#CHROM')
            gb = df.groupby('#CHROM')    
            df_split_list = [gb.get_group(x) for x in gb.groups]
            tfbs_df_list.append(df_split_list) # makes a list of lists of TFbs dfs - each TF's sites are by split chrom 
        
        else:
            excluded += 1
    
    
    total = len(tfbs_df_list) + excluded
    included = total - excluded
    print(f"{total} TFs have been filtered - {included} included, {excluded} excluded.")
            
    return tf_names, tfbs_df_list



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


