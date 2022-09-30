import numpy as np
import pandas as pd
from scipy.signal import savgol_filter


def get_sample_names(controls, cases):
    
    sample_dirs = controls + cases
    sample_names = []
    for sample_dir in sample_dirs:
        sample_name = sample_dir[25:]
        sample_names.append(sample_name)
    
    return sample_names


def extract_signal(norm_controls_cov, correct_cases_cov, tfbs_df_list, sites, bases):
    
    number_of_tf = len(tfbs_df_list)
    print(f'Getting signals for all {number_of_tf} TFs from all samples...')
    
    sample_count = 0
    norm_samples_cov = norm_controls_cov + correct_cases_cov
    sample_signals = []
    for sample in norm_samples_cov: 

        concat_signals = []
        tf_signals = []
        for tf in tfbs_df_list:
            tf_signal = []
            for j in range(len(tf)): # for every chrom df for every tf
                chrom = tf[j].index[0]
                chrom_list = load_chrom_list()
                chrom_index = chrom_list.index(chrom)
                sample_array = sample[chrom_index]
                
                length = (bases*2) + 1
                start = tf[j].ROI_start.to_numpy(int) # array of start position of tf bs on a chrom 
                end = start + length

                mask = np.zeros(len(sample_array),dtype=bool)
                for (i,j) in zip(start,end):
                    mask[i:j] = True
                signal = sample_array[mask]
                tf_signal.append(signal)
            tf_signals.append(tf_signal)
        
        for signal_list in tf_signals:
            concat_signal = np.concatenate(signal_list, axis=None).reshape(sites, length)
            concat_signals.append(concat_signal)
        sample_signals.append(concat_signals)
        sample_count += 1
        print(f'Sample {sample_count} done!')
    
    
    print('Normalizing and applying filter to signals...')
    sample_signal_means = normalize_average(sample_signals)
    samples_signal_list = filter_signal(sample_signal_means)
    print('Done!')
    
    return samples_signal_list



def get_signal_data(samples_signal_list, sample_names, screened_tfs, bases, signal_file):
    
    print('Making dataframe...')
    #start = -(bases)
    #end = bases + 1
    #nuc_range = list(range(start, end))
    
    column_arrays = []
    column_names = []
    tf_column = []
    for j in range(len(samples_signal_list[0])):
        for i in range(len(samples_signal_list)):
            column_array = samples_signal_list[i][j][1]
            column_name = sample_names[i]
            tf = screened_tfs[j]
            column_arrays.append(column_array)
            column_names.append(column_name)
            tf_column.append(tf)

    signal_df = pd.DataFrame(column_arrays).T
    index_array = [column_names, tf_column]
    cols = pd.MultiIndex.from_arrays(index_array, names=('sample', 'TF'))
    signal_df.columns = cols
    #signal_df.insert(0, 'nucleotide', nuc_range)
    signal_df.to_parquet(signal_file)
    print('Dataframe made and exported!')
    
    return signal_df



def load_chrom_list():
    
    chrom_list = ['chr1', 'chr10', 'chr11', 'chr12', 'chr13', 'chr14', 'chr15', 'chr16', 'chr17', 'chr18', 'chr19', 'chr2', 'chr20', 'chr21', 'chr22', 'chr3', 'chr4', 'chr5', 'chr6', 'chr7', 'chr8', 'chr9']
    
    return chrom_list


def normalize_average(sample_signals):
    
    sample_signal_means = []
    for sample in sample_signals:
        
        signal_means = []
        for signal in sample:
            signal_mean = np.average(signal, axis=0)/np.average(signal)
            signal_means.append(signal_mean)
        sample_signal_means.append(signal_means)
    
    return sample_signal_means


def filter_signal(sample_signal_means):
    
    samples_signal_list = []
    for sample in sample_signal_means:
        sample_signal_list = []
        for signal in sample:
            high = savgol_filter(signal, 51, 3)
            low = savgol_filter(signal, 1001, 3)
            norm = high/low
            signal_list = [signal, high, low, norm]
            sample_signal_list.append(signal_list)
        samples_signal_list.append(sample_signal_list)
        
    return samples_signal_list





