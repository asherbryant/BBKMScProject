import pandas as pd
pd.set_option('display.max_colwidth', None)
import operator
from os.path import exists


def remove_overlaps(in_directory, sites, bases, tf_list, cell_list):
    
    not_in_GTRD = []
    excluded_tfs = []
    df_list = []
    for tf in tf_list:
        
        tf_file = f'{in_directory}/{tf}.parquet'
        file_exists = exists(tf_file)
        
        # determine if requested TFs in GTRD
        if file_exists == False:
            not_in_GTRD.append(tf)
            
        else:
            df = pd.read_parquet(tf_file, columns=['#CHROM', 'START', 'cell.set', 'exp.set', 'id', 'peak-caller.count', 'peak.count','summit', 'tfTitle']).set_index('id')
            tf_name = df.tfTitle.iloc[0]
            df = df[~df['#CHROM'].isin(['chrMT','chrX', 'chrY'])]

            # retain only rows with relevant cell types 
            cell_set = df['cell.set'].tolist()
            cell_set_lists = []
            for entry in cell_set:
                cell_set_list = entry.split(';')
                cell_set_lists.append(cell_set_list)
            df['cell_set_list'] = cell_set_lists


            relevant_TFBS = []
            for cell_set_list in cell_set_lists:

                cell_intersection = len(list(set(cell_list) & set(cell_set_list)))
                if cell_intersection >= 1:
                    relevant_TFBS.append(True)
                else:
                    relevant_TFBS.append(False)
            df['relevant_TFBS'] = relevant_TFBS
            df = df.query('relevant_TFBS == True').copy()


            # remove overlapping binding sites
            if df.shape[0] >= sites:
                df['ROI_start'] = df['START'] + df['summit'] - bases
                df = df.sort_values(['#CHROM', 'ROI_start'], ascending=[True, True])

                df['ROI_start_diff'] = df.ROI_start.diff().abs()
                df['ROI_reverse_start_diff'] = df.ROI_start.diff().shift(-1)

                length = (bases*2) + 1 
                df = df.drop(df[(df['ROI_start_diff'] <= length)].index)
                df = df.drop(df[(df['ROI_reverse_start_diff'] <= length)].index)
                df_list.append(df)

            else:
                excluded_tfs.append(tf)
                       
    return df_list, excluded_tfs, not_in_GTRD



def screen_tfbs(in_directory, tf_list, cell_list, sites=1000, bases=1000):
    number_of_tfs = len(tf_list)
    print(f'Screening {number_of_tfs} TF(s)...')
          
    df_list, excluded_tfs, not_in_GTRD = remove_overlaps(in_directory, sites, bases, tf_list, cell_list)
    
    screened_tfs = []
    tfbs_df_list = []
    
    print(f"Selecting the {sites} most reliable binding sites...")
    for df in df_list:
        tf_name = df.tfTitle.iloc[0]
        
        if df.shape[0] >= sites:
            screened_tfs.append(tf_name)
            
            # retain only experiments with the most binding sites
            exp_set = df['exp.set'].tolist()
            exp_set_lists = []
            for entry in exp_set:
                exp_set_list = entry.split(';')
                exp_set_lists.append(exp_set_list)
            df['exp_set_list'] = exp_set_lists


            exp_dict = {}
            for exp_list in exp_set_lists:
                for i in exp_list:
                    if i != '...':
                        exp_dict[i] = exp_dict.get(i, 0) + 1

            sorted_exp_dict = dict(sorted(exp_dict.items(), key=operator.itemgetter(1),reverse=True))
            k_list = []
            bs = 0
            for k in sorted_exp_dict:
                if bs < 1000:
                    k_list.append(k)
                    bs += sorted_exp_dict[k]

            relevant_exp = []
            for exp_set_list in exp_set_lists:

                exp_intersection = len(list(set(k_list) & set(exp_set_list)))
                if exp_intersection >= 1:
                    relevant_exp.append(True)
                else:
                    relevant_exp.append(False)
            df['relevant_exp'] = relevant_exp
            df = df.query('relevant_exp == True').copy()
            
            
            # sortto find most reliable binding sites
            df = df.sort_values(['peak.count', 'peak-caller.count'], ascending=[False, False]).head(sites)
            #df.to_parquet(f'{tf_name}.parquet')
            
            # make dataframes for each chromosome
            df = df[['#CHROM', 'ROI_start']].set_index('#CHROM')
            gb = df.groupby('#CHROM')    
            df_split_list = [gb.get_group(x) for x in gb.groups]
            tfbs_df_list.append(df_split_list) # makes a list of lists of TFbs dfs - each TF's sites are by split chrom 

        else:
            excluded_tfs.append(tf_name)
    
    number_excluded = len(excluded_tfs)
    number_not_in_GTRD = len(not_in_GTRD)
    total = len(tfbs_df_list) + number_excluded
    tf_count = total - number_excluded
    print(f"{total} TF(s) have been screened - {tf_count} included, {number_excluded} excluded.")
    print(f"{number_not_in_GTRD} TF(s) is/are not in the GTRD database.")
    
    if number_excluded >= 1:
        excluded_tfs_str = ', '.join([str(tf) for tf in excluded_tfs])
        print(f'The following TF(s) is/are excluded: {excluded_tfs_str}.')
    
    if number_not_in_GTRD >= 1:
        not_in_GTRD_str = ', '.join([str(tf) for tf in not_in_GTRD])
        print(f'The following TF(s) is/are not in the GTRD database: {not_in_GTRD_str}.')
            
    return screened_tfs, tfbs_df_list, sites, bases, tf_count
