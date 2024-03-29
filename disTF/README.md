# disTF
disTF is a command-line tool for the analysis of cell-free DNA (cfDNA) fragment coverage[^note] in regions around cell-specific transcription factor binding sites (TFBS). It takes as input fragment coverage files generated from the [@uzh-dqbm-cmi](https://github.com/uzh-dqbm-cmi) cfDNA pipeline, [ichorCNA](https://github.com/broadinstitute/ichorCNA) copy number files and TFBS data acquired from [GTRD](http://gtrd.biouml.org:8888/downloads/current/intervals/chip-seq/). 
The analysis is carried out in two steps:
- Calculation of the Euclidean distance between case-control and control-control fragment coverage signals and plotting of the signals. 
- The Kolmogorov-Smirnov test with the Bonferroni correction for case-control distances and control-control distances.
[^note]: Genome tools (e.g. bedtools) routinely report read coverage. In paired-end sequencing, read coverage depends on the inner distance between reads. Therefore, read coverage can be zero, one, or two at a given genomic coordinate, whereas fragment coverage is always one. See [figure](read_vs_frag.png).
## Manual
### To calculate the distances:
~~~text
usage: disTF.py [-h] --tf_dir TF_DIRECTORY --tfs TF_FILE --cells CELL_FILE
                [--sites SITES] [--overlap OVERLAP] -o OUT_DIRECTORY
                --controls CONTROLS [CONTROLS ...] --cases CASES [CASES ...]
                --cnas CNA_FILES [CNA_FILES ...] [--distance DISTANCE]
                [--bases BASES] [--xticks XTICKS [XTICKS ...]]
                [--xticklabels XTICKLABELS [XTICKLABELS ...]]
~~~

#### Arguments
**TFBS Arguments**<br />
**--tf_dir**<br />
Path to the directory containing TFBS files.<br /> 
REQUIRED.<br />
**--tfs**<br />
Path to the text file to specify which TFs. See [example](TFBS/heme_tfs.txt).<br />
REQUIRED.<br />
**--cells**<br />
Path to the text file to specify which cells. See [example](TFBS/heme_cells.txt).<br /> 
REQUIRED.<br />
**--sites**<br />
Number of TFBS to be aggregated into a single signal.<br /> 
DEFAULT: `1000`.<br />
**--overlap**<br />
Number of bases from the center of another TFBS. If the center of a TFBS falls within the specified number of bases from the center of another TFBS, these TFBS will be excluded from the aggregated signal.<br />
DEFAULT: `1000`.<br />
<br />
**Sample Arguments**<br />
**-o**<br />
Path to output directory.<br />
REQUIRED.<br />
**--controls**<br />
Paths to control directories containing fragment coverage files.<br />
REQUIRED.<br />
**--cases**<br />
Paths to case directories containing fragment coverage files.<br />
REQUIRED.<br />
**--cnas**<br />
Paths to ichorCNA cna.seg files for each case.<br />
REQUIRED.<br />
**--distance**<br />
Number of bases from center of the aggregated signal to evaluate the Euclidean distance.<br />
DEFAULT: `50`.<br />
**--bases**<br />
Number of bases from the center of the TFBS to be included in output files.<br />
DEFAULT: `1000`.<br />
<br />
**Plot Arguments**<br />
**--xticks**<br />
Position of ticks on x-axis of plots. Theses values are indices. Therefore, if the `bases` argument is 1000, the indices will range from 0 to 2001.<br />DEFAULT: `[0, 499, 1000, 1501, 2001]`.<br />
**--xticklabels**<br /> 
Labels for ticks on x-axis. The number of labels should be equal to the number of `xticks`.<br />
DEFAULT: `[-1000, -500, 0, 500, 1000]`.

#### Example Usage
~~~text
TF_DIRECTORY="/path/to/GTRD/clusters"
TF_FILE="tfs.txt"
CELL_FILE="cells.txt"
OUT="/path/to/output"
CONTROLS="/path/to/control1 /path/to/control2 /path/to/control3"
CASES="/path/to/case1 /path/to/case2"
CNA_FILES="/path/to/ichorCNA/case1.cna.seg /path/to/ichorCNA/case2.cna.seg"
BASES="1500"
XTICKS="0 1500 3001"
XTICKLABELS="-1500 0 1500"

python ./disTF.py --tf_dir "$TF_DIRECTORY" --tfs "$TF_FILE" --cells "$CELL_FILE" \
-o "$OUT" --controls ${CONTROLS} --cases ${CASES} --cnas ${CNA_FILES} --bases "$BASES" \
--xticks ${XTICKS} --xticklabels ${XTICKLABELS} 
~~~

### For statistical analysis:
Run from current working directory containing multiple case sub-directories to generate a single set of files across all samples.<br />
~~~text
usage: stats.py [-h] --controls CTRLS [CTRLS ...]

Kolmogorov-Smirnov test by TF for all samples.

optional arguments:
  -h, --help            show this help message and exit
  --controls CTRLS [CTRLS ...]
                        control names
~~~ 

#### Arguments
**--controls**<br />
Names of controls.<br /> 
REQUIRED.<br />

#### Example Usage
~~~text
CONTROLS="control1 control2 control3"

python ./stats.py --controls ${CONTROLS}
~~~

## Output
`distf.py` creates a directory as specified by the `-o` argument. Within this directory, the following are created:<br />
- **signal.parquet**: fragment coverage normalized by sequencing depth and CNAs over the TFBS regions,<br />
- **product.parquet**: all unique distances between case and control samples,<br />
- **combinations.parquet**: all possible distances between case and control samples,<br />
- **pngs**: plots for each TF including all sample signals. Cases signals are colored, while control signals are shown as black, solid or dotted lines. Example shown below.<br />
<p align="left">
<img align="left" src="output_ex.png" width="600">
</p>
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />
<br />

`stats.py` creates the following files in the current working directory:<br />

- **p_values.parquet**: p-values for each TF, for each sample,<br />
- **D_stats.parquet**: D statistic for each TF, for each sample.<br />
<br />
<br />
