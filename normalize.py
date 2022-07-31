import pandas as pd
import numpy as np
import glob
from dask import dataframe as dd


def sample_cov(samples):
    # create list of arrays of base-wise coverage in each chrom for each sample
    
    print(f"Getting sample coverage...")
    samples_cov = []
    for i in range(len(samples)):
        sample_dir = samples[i]
        files = glob.glob(sample_dir + '/genome_wide*[0-9].parquet', recursive=True)
        files = sorted(files)
        
        sample_cov = []
        for file in files:
            df = dd.read_parquet(file)
            cov = df['coverage'].compute()
            cov_array = cov.to_numpy().astype(np.float32)
            sample_cov.append(cov_array)
    
        samples_cov.append(sample_cov)
    
    print("Coverage done!")
    return samples_cov


def average(samples_cov):
    
    print("Calculating weighted mean...")
    weighted_means = []
    for i in range(len(samples_cov)):
        chrom_means = [np.mean(a) for a in samples_cov[i]]
        chrom_length = [len(array) for array in samples_cov[i]]
        genome_length = sum(chrom_length)
        weights = [chrom/genome_length for chrom in chrom_length]
        weighted_mean = np.average(chrom_means, weights = weights)
        weighted_means.append(weighted_mean)
    
    print("Weighted means done!")
    return weighted_means


def norm_sample_cov(samples_cov, weighted_means, number_of_controls, number_of_cases):
    
    print("Normalizing sample coverage...")
    norm_list = []
    for i in range(len(samples_cov)):
        norm_sample_cov = [np.divide(array, weighted_means[i]) for array in samples_cov[i]]
        norm_list.append(norm_sample_cov)
    
    norm_controls_cov = norm_list[:number_of_controls]
    norm_cases_cov = norm_list[-number_of_cases:]
    
    print("Control samples normalized!")
    return norm_controls_cov, norm_cases_cov


def ichorCNA_norm(cna_files):
    # get the ichorcna file, drop unwanted columns/rows, create normalize cn column
    
    print("Getting cases sample copy number variation...")
    ichorCNA_start_list = []
    ichorCNA_cn_list = []
    for i in range(len(cna_files)):
        df = pd.read_csv(cna_files[i], delimiter='\t', usecols=[0,1,2,7]).iloc[:, [0,1,2,3]]
        df = df[~df['chr'].isin(['X'])]
        df['norm_cn'] = df.iloc[:, 3].apply(lambda row: row/2)
        df = df.drop(df.columns[[3]], axis=1)
        df.loc[df.norm_cn == 0, 'norm_cn'] = 0.001
        
        # get bin size
        bin_size = df.end[0] - df.start[0]

        # split df into chr dfs
        df_list = []
        gb = df.groupby('chr')    
        df_split_list = [gb.get_group(x) for x in gb.groups]
        df_list.append(df_split_list)
        
        # create bin start position & copy number arrays
        start_array_list = []
        cn_array_list = []
        for i in range(len(df_list[0])):
            df = df_list[0][i]
            start = df.start.to_numpy()
            copy_number = df.norm_cn.to_numpy()
            start_array_list.append(start)
            cn_array_list.append(copy_number)
        
        ichorCNA_start_list.append(start_array_list)
        ichorCNA_cn_list.append(cn_array_list)
    
    print("Got copy number variation!")
    return ichorCNA_start_list, ichorCNA_cn_list, bin_size
        

def correct_case_cov(norm_cases_cov, ichorCNA_start_list, ichorCNA_cn_list, bin_size):
    
    print("Correcting for copy number variation in control samples...")
    correct_cases_cov = []
    for i in range(len(norm_cases_cov)):   
        cn_norm_case_cov = []
        for j in range(len(norm_cases_cov[i])): 
            chrom_array = norm_cases_cov[i][j].copy().astype(np.float32)
            for k in range(len(ichorCNA_start_list[i][j])): 
                start = ichorCNA_start_list[i][j][k] - 1 # change to index
                end = start + bin_size
                cn = ichorCNA_cn_list[i][j][k]
                chrom_array[start:end] = chrom_array[start:end]/cn
            cn_norm_case_cov.append(chrom_array) # outdented b/c changes to array have to accumlate BEFORE added to array list
        correct_cases_cov.append(cn_norm_case_cov)
    
    print("Control samples normalized!")
    return correct_cases_cov


def nomrmalize(controls, cases, cna_files):
    
    number_of_controls = len(controls)
    number_of_cases = len(cases)
    number_of_cna_files = len(cna_files)
        
    if number_of_cases != number_of_cna_files:
        raise Exception(f'There is/are {number_of_cases} case sample(s), but {number_of_cna_files} ichorCNA file(s).\nEnsure there for each case there is a ichorCNA file & vice versa.')
        
    else:
        print(f'Got {number_of_controls} control and {number_of_cases} case samples!')
    
    
    samples = controls + cases
    samples_cov = sample_cov(samples)
    
    weighted_means = average(samples_cov)
    
    norm_controls_cov, norm_cases_cov = norm_sample_cov(samples_cov, weighted_means, number_of_controls, number_of_cases)
    
    ichorCNA_start_list, ichorCNA_cn_list, bin_size = ichorCNA_norm(cna_files)
    
    correct_cases_cov = correct_case_cov(norm_cases_cov, ichorCNA_start_list, ichorCNA_cn_list, bin_size)
    
    number_of_samples = len(norm_controls_cov) + len(correct_cases_cov)
    
    print(f"Normalization of {number_of_samples} samples complete!")
    return norm_controls_cov, correct_cases_cov


    