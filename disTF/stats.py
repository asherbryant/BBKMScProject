#! /usr/bin/env python

import argparse
import glob
import pandas as pd
from scipy import stats

def run(args):
    # get all disTF files
    product_files = sorted(glob.glob('**/*product.parquet', recursive=True))
    combo_files = sorted(glob.glob('**/*combinations.parquet', recursive=True))

    # get tfs
    df = pd.read_parquet(product_files[0])
    tfs = df['TF'].unique().tolist()
    
    # create ctrl dfs
    ctrl_D_stats = []
    ctrl_p_values = []
    for ctrl in args.ctrls:
        D_stats = []
        p_values = []
        for tf in tfs:
            other_ctrls = [x for x in args.ctrls if x != ctrl]
            product_df = pd.read_parquet(product_files[0]) # use product file to ensure have all pairing
            ctrl_d = product_df.query(f"TF  == '{tf}' & sample_1 == '{ctrl}' & sample_2 in {other_ctrls} ")['distance']

            combo_df = pd.read_parquet(combo_files[0]) # use combo file to ensure not repeats
            other_d = combo_df.query(f"TF == '{tf}' & sample_1 in {other_ctrls} & sample_2 in {other_ctrls} ")['distance']

            D, p =stats.ks_2samp(ctrl_d, other_d, alternative='less')
            p_values.append(p * len(tfs))
            D_stats.append(D)
        ctrl_D_stats.append(D_stats)
        ctrl_p_values.append(p_values)

    ctrl_p_df = pd.DataFrame(ctrl_p_values, columns = tfs)
    ctrl_p_df.index = args.ctrls

    ctrl_D_df = pd.DataFrame(ctrl_D_stats, columns = tfs)
    ctrl_D_df.index = args.ctrls
    
    # create case dfs
    case_D_stats = []
    case_p_values = []
    all_case_samples = []
    for i in range(len(product_files)):
        # get case sample names
        df = pd.read_parquet(product_files[i])
        samples = df['sample_1'].unique().tolist()
        case_samples = [x for x in samples if x not in args.ctrls]
        all_case_samples.extend(case_samples) # for index of dfs


        for sample in case_samples:
            D_stats = []
            p_values = []
            for tf in tfs:
                product_df = pd.read_parquet(product_files[i]) # use product file to ensure have all pairing
                case_d = product_df.query(f"TF  == '{tf}' & sample_1 == '{sample}' & sample_2 in {args.ctrls} ")['distance']

                combo_df = pd.read_parquet(combo_files[i]) # use combo file to ensure not repeats
                ctrl_d = combo_df.query(f"TF == '{tf}' & sample_1 in {args.ctrls} & sample_2 in {args.ctrls} ")['distance']

                D, p =stats.ks_2samp(case_d, ctrl_d, alternative='less')
                p_values.append(p * len(tfs))
                D_stats.append(D)
            case_D_stats.append(D_stats)
            case_p_values.append(p_values)

    case_p_df = pd.DataFrame(case_p_values, columns = tfs)
    case_p_df.index = all_case_samples

    case_D_df = pd.DataFrame(case_D_stats, columns = tfs)
    case_D_df.index = all_case_samples
    
    # combine case & ctrl dfs
    p_df = pd.concat([case_p_df, ctrl_p_df])
    D_df = pd.concat([case_D_df, ctrl_D_df])
    
    # export stat files
    p_df.to_parquet('p_values.parquet')
    D_df.to_parquet('D_stats.parquet')
    
def main():
    parser=argparse.ArgumentParser(description="Kolmogorov-Smirnov test by TF for all samples.")
    
    parser.add_argument("--controls", help="control names", nargs='+', dest="ctrls", type=str, required=True)
    
    parser.set_defaults(func=run)
    args=parser.parse_args()
    args.func(args)

if __name__=="__main__":
    main()