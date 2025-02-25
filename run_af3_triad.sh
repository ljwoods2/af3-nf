#!/bin/bash
#SBATCH --job-name=alphafold
#SBATCH --mail-type=ALL
#SBATCH --mail-user=lwoods@tgen.org
#SBATCH --ntasks=1
#SBATCH --mem=3G
#SBATCH --time=24:00:00
#SBATCH --output=run_alpha.%j.log

module load Java/11.0.2


nextflow run \
    -w /scratch/lwoods/work \
    -c /home/lwoods/workspace/5LR-py/alphafold/nextflow.config \
    /home/lwoods/workspace/5LR-py/alphafold/af3_triad_msa.nf \
        --input_csv '/home/lwoods/workspace/5LR-py/alphafold/queries/AF3queries5.csv' \
        --out_dir '.' \
        --msa_db '/tgen_labs/altin/alphafold3/msa.db' 


nextflow run \
    -w /scratch/lwoods/work \
    -c /home/lwoods/workspace/5LR-py/alphafold/nextflow.config \
    /home/lwoods/workspace/5LR-py/alphafold/af3_triad_inference.nf \
        --input_csv '/home/lwoods/workspace/5LR-py/alphafold/queries/AF3queries5.csv' \
        --out_dir '.' \
        --msa_db '/tgen_labs/altin/alphafold3/msa.db' 
