#! /usr/bin/env python

# script creates coverage and WPS gene-wise matrices
#-----------------------------------------------------

import pandas as pd
import glob

# get Human Protein Atlas genes file
genes_dir = '/mnt/DATA1/resources/genes'
genes_files = glob.glob(genes_dir + "/*chr*", recursive=True)
genes_files = sorted(genes_files)


# get sample files
samples = ['HV01', 'HV03', 'HV04', 'HV05', 'HV06', 'HV07', 'HV08']
for sample in samples:
    sample_dir = f'/mnt/DATA1/Radiooncology/{sample}'
    sample_files = glob.glob(sample_dir + '/*genome_wide_chr*.parquet', recursive=True)
    sample_files = sorted(sample_files)
    
    
    # read in the files, create list of start positions & gene ids
    bases = 3000
    wps_df_list = []
    coverage_df_list = []
    for i in range(len(genes_files)):
        chr_genes_df = pd.read_parquet(genes_files[i])
        chr_genes_start = chr_genes_df['start'].tolist()
        chr_gene_ids = chr_genes_df.index.values.tolist()
        sample_df = pd.read_parquet(sample_files[i])
        sample_df.set_index('nucleotide', inplace=True)

        # create list of lists for each gene for WPS & coverage
        wps_list = []
        coverage_list = []
        for start in chr_genes_start:
            upstream = start - bases
            downstream = start + bases
            wps_values = sample_df.loc[upstream:downstream]['wps'].tolist()
            wps_list.append(wps_values)
            coverage_values = sample_df.loc[upstream:downstream]['coverage'].tolist()
            coverage_list.append(coverage_values)

        # create a dictionaries with genes ids as keys, wps & coverage as values
        wps_dict = dict(zip(chr_gene_ids, wps_list))
        wps_df = pd.DataFrame(wps_dict) 
        wps_df_list.append(wps_df)

        coverage_dict = dict(zip(chr_gene_ids, coverage_list))
        coverage_df = pd.DataFrame(coverage_dict) 
        coverage_df_list.append(coverage_df)


    # create dataframes for gene-wise WPS & coverage
    wps_concat = pd.concat(wps_df_list, axis=1)  
    coverage_concat = pd.concat(coverage_df_list, axis=1)


    # dataframes to parquet files
    wps_concat.to_parquet(f'{sample}_wps_{bases}.parquet')
    coverage_concat.to_parquet(f'{sample}_cov_{bases}.parquet')