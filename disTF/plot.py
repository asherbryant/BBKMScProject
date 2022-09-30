
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def plot_tfbs(signal_df, sample_names, number_of_controls, tf_list, out_directory, xticks = [0, 499, 1000, 1501, 2001], xticklabels = [-1000, -500, 0, 500, 1000]):
    
    number_of_plots = len(tf_list)
    print(f'Making {number_of_plots} plots...')
    
    for tf in tf_list:
        controls_df = signal_df.loc[:, (sample_names[:number_of_controls], tf)]
        cases_df = signal_df.loc[:, (sample_names[number_of_controls:], tf)]
        
        plt.rcParams['figure.figsize'] = (10,7)
        
        fig, ax = plt.subplots()
        fig.suptitle(tf, fontsize=16)
        
        a = sns.lineplot(data = controls_df, palette ='binary_r', dashes=True, ax=ax)
        b = sns.lineplot(data = cases_df, palette = 'Dark2', dashes=False, ax=ax)
        
        a.set_xticks(xticks)
        a.set_xticklabels(xticklabels)
        
        plt.legend(loc='upper right')
        sns.despine()
        
        plt.savefig(f'{out_directory}/{tf}.png', dpi=300)
        
    print('Plots made and exported!')




