import numpy as np
import pandas as pd
import itertools
from dtaidistance import ed


def create_args(samples_signal_list, screened_tfs, start=949, end=1050):
    
    signal_range = slice(start,end)
    # flatten samples_signal_list so same tf signal of all 9 samples are together
    args = []
    for k in range(len(samples_signal_list[0])):
        for i in range(len(samples_signal_list)):
            arg = samples_signal_list[i][k][1][signal_range]
            args.append(arg)

    # group tf signals into separate lists
    tf_count = len(screened_tfs)
    grouped_args_array = np.array_split(args, tf_count)
    grouped_args = [list(array) for array in grouped_args_array]
    
    return grouped_args


def get_combinations(samples_signal_list, sample_names, screened_tfs, combinations_file):
    
    grouped_args = create_args(samples_signal_list, screened_tfs)
    
    disTF_lists = []

    for i in range(len(screened_tfs)):
        tf = screened_tfs[i]
        sample_combos = list(itertools.combinations(sample_names, 2))
        signal_combos = list(itertools.combinations(grouped_args[i], 2))
        length = len(list(sample_combos))
        for j in range(length):
            samples = list(sample_combos[j])
            pair = signal_combos[j]
            s1 = pair[0]
            s2 = pair[1]
            distance = ed.distance(s1,s2)
            disTF = [tf, samples[0], samples[1], distance]
            disTF_lists.append(disTF)
    
    combinations_df = pd.DataFrame(disTF_lists, columns = ['TF', 'sample_1', 'sample_2','distance'])
    combinations_df.to_parquet(combinations_file)
    
    return combinations_df


def get_product(samples_signal_list, sample_names, screened_tfs, product_file):
    
    grouped_args = create_args(samples_signal_list, screened_tfs)
    
    disTF_lists = []

    for i in range(len(screened_tfs)):
        tf = screened_tfs[i]
        sample_combos = list(itertools.product(sample_names, repeat=2))
        signal_combos = list(itertools.product(grouped_args[i], repeat=2))
        length = len(list(sample_combos))
        for j in range(length):
            samples = list(sample_combos[j])
            pair = signal_combos[j]
            s1 = pair[0]
            s2 = pair[1]
            distance = ed.distance(s1,s2)
            disTF = [tf, samples[0], samples[1], distance]
            disTF_lists.append(disTF)
    
    product_df = pd.DataFrame(disTF_lists, columns = ['TF', 'sample_1', 'sample_2','distance'])
    product_df.to_parquet(product_file)
    
    return product_df


def get_ranks(product_df, sample_names, number_of_controls, ranks_file):
    
    control_names = sample_names[:number_of_controls]
    case_names = sample_names[number_of_controls:]

    df = product_df
    tf_gb = df.groupby('TF')    
    tf_df_list = [tf_gb.get_group(x) for x in tf_gb.groups]
    
    sample_min_distances = []
    for case in case_names:
        ctrl_dfs = [df.query(f"sample_1 == '{case}' & sample_2 in {control_names}") for df in tf_df_list]

        min_distances = []
        tf_names = []
        for tf in ctrl_dfs:
            tf_name = tf.TF.iloc[0]
            min_distance = tf.distance.min()
            tf_names.append(tf_name)
            min_distances.append(min_distance)
        sample_min_distances.append(min_distances)
    
    dis_dict = dict(zip(case_names, sample_min_distances))
    dis_df = pd.DataFrame(dis_dict)
    dis_df.insert(0, 'TF', tf_names)
    ranks_df = dis_df.sort_values(by=case_names[0], ascending=False).reset_index(drop=True)
    
    
    ranks_df.to_parquet(ranks_file)
    
    return ranks_df
    
    