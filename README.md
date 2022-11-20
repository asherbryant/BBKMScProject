# Hematopoietic cell-free DNA distinguishes cancer patients with low tumor fraction from healthy controls

## About this Repository
This repository documents the work for my Bioinformatics MSc Project module at [**Birkbeck, University of London**](https://www.bbk.ac.uk/). This work was undertaken over the course of a semester in the [**Krauthammer Lab**](https://krauthammerlab.ch/) at the University of Zurich, where my primary supervisor was Zsolt Balázs, MD, PhD. 

## Project Abstract
Liquid biopsies of blood cell-free DNA (cfDNA) and circulating tumor DNA (ctDNA) enable minimally invasive cancer detection and monitoring. Various cfDNA and ctDNA analytic techniques readily detect cancer in samples when tumor fraction, the fraction of ctDNA to cfDNA in a sample, is high. However, cancer is often not detected in samples with low tumor fraction. Here, we show that hematopoietic cfDNA is a potential cancer biomarker in samples with low tumor fraction. In our prospective, radiotherapy cohort of 21 cases and 7 controls, we find that the hematopoietic DNAse I hypersensitivity site (DHS) signals are significantly different in cancer samples with low tumor fraction compared to control samples. These signals also explain most of the variance in our cohort. In addition, our reanalysis of data from a published cohort of 86 pediatric sarcoma samples with low tumor fraction reveals significant differences between control and case hematopoietic DHS signals at diagnosis, therapy, relapse and remission. Furthermore, signals of hematopoietic transcription factor binding sites (TFBS) of 41 out of 56 of our low tumor fraction samples are significantly different from controls. These findings demonstrate the value of non-tumor cfDNA in cancer detection.

## Graphical Summary of Methods
<p align="center">
<img src="methods_summary.png" width="800">
</p>

## Respository Contents
In reference to the graphical summary of methods above, the repository contents include:<br />
**(A/B)** [Information](sample_info.csv) about the study cohort and samples;<br />
**(C)** ichorCNA<sup>1</sup> and t-MAD<sup>2</sup> copy number abberration (CNA) [plots](output);<br />
**(D)** LIQUORICE<sup>3</sup> DHS [data output and plots](output/LIQUORICE/), [script](PCA) for the PCA of dip depth z-scores, [scripts](Peneder) for the reanalysis of a pediatric sarcoma cohort<sup>4</sup>;<br />
**(E)** TFBS  [command-line program](disTF) and [data output](output/TFBS/), [script](TFBS/split_GTRD.py) to split [GTRD<sup>5</sup> ChiP-seq data](http://gtrd.biouml.org:8888/downloads/current/intervals/chip-seq/) by TF;<br />
**(F)** [Scripts](coverage_WPS) comparing fragment coverage and windowed protection score (WPS) at promoter regions.<br />
<br />

### References
1. Adalsteinsson, V. A. et al. Scalable whole-exome sequencing of cell-free DNA reveals high concordance with metastatic tumors. Nature Communications 8, 1–13 (2017).<br />
2. Mouliere, F. et al. Enhanced detection of circulating tumor DNA by fragment size analysis. Science Translational Medicine 10, (2018).<br />
3. Peneder, P., Bock, C. & Tomazou, E. M. LIQUORICE: detection of epigenetic signatures in liquid biopsies based on whole-genome sequencing data. Bioinformatics Advances 2, (2022).<br />
4. Peneder, P. et al. Multimodal analysis of cell-free DNA whole-genome sequencing for pediatric cancers with low mutational burden. Nature Communications 12, (2021).<br />
5. Kolmykov, S. et al. GTRD: an integrated view of transcription regulation. Nucleic Acids Research 49, D104–D111 (2020).
