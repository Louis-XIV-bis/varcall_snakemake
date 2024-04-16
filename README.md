Snakemake variant calling pipeline for HPC
======================

## Description

This project is an analysis pipeline using [Snakemake](https://snakemake.readthedocs.io/en/stable/) for variant calling (initially for yeasts).
This pipeline  specificity is to **merge same strains coming from the same paper into one individual**. Therefore, it may not apply to some species (such as humans). 

The pipeline was developped for [SLURM](https://slurm.schedmd.com/documentation.html) based HPC cluster but can be run on any cluster infrastructure (or locally) if the parameters are changed accordingly. 

The main steps of the pipeline are:
- downloading the fastq files from [ENA](https://www.ebi.ac.uk/ena/browser/home)
- alignment of reads on reference genome with [bwa](http://bio-bwa.sourceforge.net/)
- merging the same strains from a given paper with [samtools merge](https://www.htslib.org/doc/samtools-merge.html)
- marking of duplicates with [gatk MarkDuplicatesSpark](https://gatk.broadinstitute.org/hc/en-us/articles/360037224932-MarkDuplicatesSpark)
- variant calling with [gatk HaplotypeCaller](https://gatk.broadinstitute.org/hc/en-us/articles/360037225632-HaplotypeCaller)
- merging per strain gVCF to population gVCF with [gatk CombineGVCFs](https://gatk.broadinstitute.org/hc/en-us/articles/360037053272-CombineGVCFs)
- converting gVCF to VCF with [gatk GenotypeGVCFs](https://gatk.broadinstitute.org/hc/en-us/articles/360037057852-GenotypeGVCFs)
- removing repeated regions with [gatk VariantFiltration](https://gatk.broadinstitute.org/hc/en-us/articles/360037434691-VariantFiltration) and [vcftools](https://vcftools.github.io/index.html).

The pipeline takes as input a list of **ENA IDs** for which you can get read files (*e.g.* https://www.ebi.ac.uk/ena/browser/view/PRJEB13017). Each time you add a new ENA ID to the config file and re-run the pipeline, it will add the new samples to the final VCF. It will only keeps as intermediate files the per strain gVCF in order not to have to generate them again for each new run of the pipeline. 

Here is a representation of the pipeline:    
  
![Logo](/plot_readme/DAG_pipeline.png)

  
## System requirements

The only requirement is to be on a SLURM HPC cluster and have a working install of [conda](https://www.anaconda.com/download/#linux) and [git](https://git-scm.com/downloads).
All tools necessary to run the pipeline are described in a conda environment file.  

The species specific resources files have to be downloaded manually if not *S. cerevisiae*. 

## Usage 
### Initialization
These commands have to be run only once to setup the pipeline.

#### Cloning the git repository
```
git clone "https://gitlab.lisn.upsaclay.fr/lollivier/variant-calling-pipeline"
cd variant-calling-pipeline
```

#### Create the appropriate environment using the conda export file provided
```
conda env create -f workflow/envs/environment.yaml -n your_env_name
conda activate your_env_name 
```

#### If SLURM : create your profile 

In order to run the pipeline on SLURM cluster, you need to create a "profile" file contains information about the resources and other snakemake commands. The profile should be in the folllowing folder: $HOME/.config/snakemake/name_of_profile. You can name the profile the way you want but will need to use that name to run the pipeline. More information [here](https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles).  

Now, if you need to run the pipeline on a SLURM based cluster (recommended) or on a local computer, follow the according section.

The file used already exist in the **/workflow/profile/** directory. 


```
mkdir $HOME/.config/snakemake/name_of_profile
cp workflow/profile/config_slurm.yaml $HOME/.config/snakemake/name_of_profile/config.yaml
```

You can change the profile file according to your preferences. 


### Running the pipeline

In order to run the pipeline, you need to give some ENA IDs in the **config/config.yaml** file. If you want to add a new ENA ID, just add it to the existing ENA ID list. It will keep in storage the intermediate file not to have to run it again (only the new IDs and the merge of gVCF). If you remove some IDs from the list, the gVCF will still be on the storage but not used for the final VCF. 

Each time you add a new ENA ID to the config file, you'll to run the next two commands in order (python -> snakemake).

#### Generate intermediate files for the pipeline

This script will download the metadata for each new ENA ID and format it to use in the pipeline.

```
python generate_tables.py
```

#### Run the pipeline

The pipeline is made in a way to prioritize the jobs strain by strain. Then, for new ENA ID, it will run multiple strains in parallel for this new ID. That way, it prevents from keep in storage too much intermediate files (BAM, SAM, etc). Only final gVCF for each strain is kept.   

Follow the correct section if you want to run the pipeline on a SLURM HPC cluster (recommended) or on a local computer.   

The process will run in the background using **nohup** and **&**. You can see the progress in the **nohup.out** generated file. 

##### SLURM HPC cluster 

```
nohup snakemake --profile name_of_profile &
```
##### Local computer

```
nohup snakemake --resources mem_mb=64000 --cores 8 &
```
Note: you can change the values for the RAM ad the number of core. You can also create a profile to specify more resources but you'd need to change the script for each rule.

