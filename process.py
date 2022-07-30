import numpy as np
import pandas as pd


def load_chrom_index():
    
    chrom_list = ['chr1', 'chr10', 'chr11', 'chr12', 'chr13', 'chr14', 'chr15', 'chr16', 'chr17', 'chr18', 'chr19', 'chr2', 'chr20', 'chr21', 'chr22', 'chr3', 'chr4', 'chr5', 'chr6', 'chr7', 'chr8', 'chr9']
    
    return chrom_list



def extract(norm_controls_cov, correct_cases_cov, sites, length):
    
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
        
    return sample_signals


def average(sample_signals):
    
    sample_signal_means = []
    for sample in sample_signals:
        
        signal_means = []
        for signal in sample:
            signal_mean = np.average(signal, axis=0)
            signal_means.append(signal_mean)
        sample_signal_means.append(signal_means)
    
    return sample_signal_means