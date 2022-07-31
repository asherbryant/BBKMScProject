import glob
import pandas as pd
from dask import dataframe as dd


def load_high_spec_tfs():
    high_spec_file = 'high-specificity.txt'
    with open(high_spec_file, 'r') as file:
        high_spec_tfs = file.read().splitlines()
    
    return high_spec_tfs


def remove_overlaps(in_directory, sites, bases, high_spec):
    tf_files = glob.glob(in_directory + "/*.parquet", recursive=True)
    number_of_files = len(tf_files)
    print(f"Got {number_of_files} TF files!")
    print(f"Removing overlaps...")
    
    if high_spec == True:
        high_spec_list = load_high_spec_tfs()
    else:
        high_spec_list = [tf_name]
    
    
    df_list = []
    for i in range(number_of_files):   
        df = pd.read_parquet(tf_files[i]).set_index('id')
        tf_name = df.tfTitle.iloc[0]
        df = df[~df['#CHROM'].isin(['chrMT','chrX', 'chrY'])]
        

        if df.shape[0] >= sites and tf_name in high_spec_list:
            df['ROI_start'] = df['START'] + df['summit'] - bases
            df = df.sort_values(['#CHROM', 'ROI_start'], ascending=[True, True])

            df['ROI_start_diff'] = df.ROI_start.diff().abs()
            df.iat[0,-1] = df.iat[1,-1] # first row values

            length = (bases*2) + 1
            df = df.drop(df[(df['ROI_start_diff'] <= length)].index)
            df_list.append(df)

        else:
            continue
    
    return df_list


def screen_tfbs(in_directory, sites=1000, bases=1000, high_spec=True):
    
    df_list = remove_overlaps(in_directory, sites, bases, high_spec)
    
    screened_tfs = []
    tfbs_df_list = []
    excluded = 0
    
    print(f"Selecting the {sites} most reliable binding sites...")
    for df in df_list:
        if df.shape[0] >= sites:
            tf_name = df.tfTitle.iloc[0]
            screened_tfs.append(tf_name)
            df = df.sort_values('peak-caller.count',ascending = False).head(1000)
            df = df[['#CHROM', 'ROI_start']].set_index('#CHROM')
            gb = df.groupby('#CHROM')    
            df_split_list = [gb.get_group(x) for x in gb.groups]
            tfbs_df_list.append(df_split_list) # makes a list of lists of TFbs dfs - each TF's sites are by split chrom 

        else:
            excluded += 1
    
    total = len(tfbs_df_list) + excluded
    included = total - excluded
    print(f"{total} TFs have been filtered - {included} included, {excluded} excluded.")
            
    return screened_tfs, tfbs_df_list, sites, bases





