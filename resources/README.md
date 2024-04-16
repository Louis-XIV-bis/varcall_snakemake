# Project resources

This **resources** folder contains essential data for the project, organized into two subfolders: **ref** (for reference genome) and  and **rep_regions**  (for repeated regions file).

## Reference Genome

The reference genome subfolder holds the reference genome files necessary for genomic analysis.

See **ref/README.md** for more information about how files were obtained.

## Repeated Regions

The repeated regions subfolder contains files related to repeated genomic regions. The resulting file (**rep_regions_Scere.bed**) is used at the end of the pipeline to remove identified repeated regions in the *S. cerevisiae* genome from the VCF file. 

It is possible for you tu use another file as long as it is in BED format and have the same name. 

### Obtaining Repeated Regions Files

Two different repeated were used (combined into a unique one) present in the **rep_regions/init_files/** folder: 

- From this paper: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4059241/ (supp data, 100nt folder), the chromosome names were change to match with the reference chromosome names  
- From using RepeatMasker tool on the 

From the 2 files in **init_file** folder, the following commands were used: 
```
sort -k1,1 -k2,2n genome_repeatMasker_fromSam.fa.bed > genome_repeatMasker.sorted.bed  
sort -k1,1 -k2,2n Position_uniquemap_Jubin2014.bed > Position_uniquemap_Jubin2014.sorted.bed  
bedtools merge -i Position_uniquemap_Jubin2014.sorted.bed -i genome_repeatMasker.sorted.bed > rep_regions_Scere.bed  
```
## Note
The commands assume that you have activated the associated conda environment (see main README.md).