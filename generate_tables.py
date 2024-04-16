# Author: Louis OLLIVIER (louis.xiv.bis@gmail.com)
# Date : February 2023 

import numpy as np
import yaml, json
import sys, os

sys.path.append("workflow/scripts/")
from functions import *

def main():
    
    ############# Downloading, merging and filtering the information files #############
    # Download all the tsv files from the ENA IDs. They contain all the information
    # requiered for the rest of the pipeline (sample ID, ftp link to dl fastq, etc).
    # After that, it's merged into one unique file and filtered / processed (e.g. keep
    # only S.cere sequences). Then, a csv table is created for each ENA_strain ID from the table.
    
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    ENA = config["ENA_ID_get_gvcf"]  # list of ENA IDs
    tax_id = config["tax_id"]  # taxon id is specific for each species

    # Check if the output directory exists, if not, create it otherwise 
    # remove the existing file to avoid conflict 
    results_dir = "./results/table/"

    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    else: 
        for file in os.listdir(results_dir):
            file_path = os.path.join(results_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                
    # Download, merge and process the tables into one unique table 
    dl_tsv_ENA(ENA, results_dir)
    merge_tsv_files(results_dir, "merged_table.tsv.ok")
    process_table(results_dir, "merged_table.tsv.ok", "merged_filtered_table.csv", tax_id)

    # Split the table to reate the files for each ENA_strain and the associated list to use for the pipeline
    ENA_strain_list = split_and_save_csv(results_dir, "merged_filtered_table.csv", )

    # # Check if the pipeline already ran (and produced a list of ENA_strain IDs), if yes, 
    # # add the existing list to the new list in order to have the correct input for the pipeline (all strain_IDs)
    ENA_strain_path = results_dir + "ENA_strain_list.json"
    with open(ENA_strain_path, 'w') as file:
        ENA_strain_list = ENA_strain_list.tolist()
        json.dump(ENA_strain_list, file)

if __name__ == "__main__":
    main()