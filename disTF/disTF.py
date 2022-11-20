#! /usr/bin/env python

import argparse
import normalize
import screen
import process
import analyze
import plot
import os


def run(args):
    with open(args.tf_file, 'r') as file:
            tf_list = file.read().splitlines()

    with open(args.cell_file, 'r') as file:
            cell_list = file.read().splitlines()

    screened_tfs, tfbs_df_list, tf_count = screen.screen_tfbs(args.tf_directory, tf_list, cell_list, args.sites, args.bases, args.overlap)

    if not os.path.exists(f"{args.out_directory}"):
        os.makedirs(f"{args.out_directory}")

    sample_names = process.get_sample_names(args.controls, args.cases)

    norm_controls_cov, correct_cases_cov, number_of_controls = normalize.normalize(args.controls, args.cases, args.cna_files)

    samples_signal_list = process.extract_signal(norm_controls_cov, correct_cases_cov, tfbs_df_list, args.sites, args.bases)
    
    start = args.bases - (args.distance -1)
    end = args.bases - (args.distance + 1)

    case = args.out_directory.rsplit('/', 1)[-1]
    product_file = f'{args.out_directory}/{case}_product.parquet'
    product_df = analyze.get_product(samples_signal_list, sample_names, screened_tfs, product_file, start, end)

    combinations_file = f'{args.out_directory}/{case}_combinations.parquet'
    combinations_df = analyze.get_combinations(samples_signal_list, sample_names, screened_tfs, combinations_file, start, end)

    signal_file = f'{args.out_directory}/{case}_signal.parquet'
    signal_df = process.get_signal_data(samples_signal_list, sample_names, screened_tfs, args.bases, signal_file)

    plot.plot_tfbs(signal_df, sample_names, number_of_controls, screened_tfs, args.out_directory, args.xticks, args.xticklabels)

def main():
    parser=argparse.ArgumentParser(description="Analyze the Euclidean distance between case and control fragment coverage signals and generate the signal plots.")
    
    # TFBS arguments
    parser.add_argument("--tf_dir", help="directory of GTRD TF files", dest="tf_directory", type=str, required=True)
    parser.add_argument("--tfs", help="text file of specified TFs", dest="tf_file", type=str, required=True)
    parser.add_argument("--cells", help="text file of specified cell types", dest="cell_file", type=str, required=True)
    parser.add_argument("--sites", help="number of TFBSs to be aggregated into a signal", dest="sites", type=int, default=1000)
    parser.add_argument("--overlap", help="number of bases from the center of another TFBS", dest="overlap", type=int, default=1000)
    
    # sample arguments
    parser.add_argument("-o", help="output directory", dest="out_directory", type=str, required=True)
    parser.add_argument("--controls", help="control directories", nargs='+', dest="controls", type=str, required=True)
    parser.add_argument("--cases", help="case directories", nargs='+', dest="cases", type=str, required=True)
    parser.add_argument("--cnas", help="ichorCNA files", nargs='+', dest="cna_files", type=str, required=True)
    parser.add_argument("--distance", help="number of bases from center of TFBS for distance calculation", dest="distance", type=int, default=50)
    parser.add_argument("--bases", help="number of bases from the center of TFBS for output files", dest="bases", type=int, default=1000)
    
    # plot arguments
    parser.add_argument("--xticks", help="position of ticks on x-axis", dest="xticks", nargs='+', type=int, default=[0, 499, 1000, 1501, 2001])
    parser.add_argument("--xticklabels", help="labels for ticks on x-axis", dest="xticklabels", nargs='+', type=int, default=[-1000, -500, 0, 500, 1000])
   
    parser.set_defaults(func=run)
    args=parser.parse_args()
    args.func(args)
    
if __name__=="__main__":
    main()

