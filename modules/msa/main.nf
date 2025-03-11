


process filterMissingMSA {
    queue 'compute'
    executor "slurm"
    tag "${proteinType}_${seq}"
    debug true

    input:
    tuple val(species), val(proteinType), val(seq), val(chain), val(name), val(proteinClass)

    output:
    path "*.csv", optional: true

    script:
    def chain_arg = chain ? "--chain ${chain}" : ""
    def name_arg = name ? "--name ${name}" : ""
    def class_arg = name ? "--protein_class ${proteinClass}" : ""
    """
    module load singularity

    fname=\$(uuidgen).csv

    export SINGULARITYENV_VAST_S3_ACCESS_KEY_ID="\$VAST_S3_ACCESS_KEY_ID"
    export SINGULARITYENV_VAST_S3_SECRET_ACCESS_KEY="\$VAST_S3_SECRET_ACCESS_KEY"

    singularity exec --nv \\
        -B /home,/scratch,/tgen_labs --cleanenv \\
        /tgen_labs/altin/alphafold3/containers/msa-db.sif \\
        python ${workflow.projectDir}/modules/msa/filter_missing_msa.py \\
            -t "$proteinType" \\
            -s "$seq" \\
            -sp "$species" \\
            $chain_arg \\
            $name_arg \\
            $class_arg \\
            -db "$params.msa_db" \\
            -o "\$fname"
    """
}

    // . "/home/lwoods/miniconda3/etc/profile.d/conda.sh"
    // conda activate vast-db

process storeMSA {
    queue 'compute'
    executor "slurm"
    tag "${proteinType}_${seq}"
    
    input:
    tuple val(species), val(proteinType), val(seq), val(chain), val(name), val(proteinClass), path(json)

    script:
    """
    module load singularity

    export SINGULARITYENV_VAST_S3_ACCESS_KEY_ID="\$VAST_S3_ACCESS_KEY_ID"
    export SINGULARITYENV_VAST_S3_SECRET_ACCESS_KEY="\$VAST_S3_SECRET_ACCESS_KEY"
    
    singularity exec --nv \\
        -B /home,/scratch,/tgen_labs --cleanenv \\
        /tgen_labs/altin/alphafold3/containers/msa-db.sif \\
        python ${workflow.projectDir}/modules/msa/store_msa.py \\
            -t "$proteinType" \\
            -s "$seq" \\
            -sp "$species" \\
            --chain "$chain" \\
            --name "$name" \\
            --protein_class "$proteinClass" \\
            -db "$params.msa_db" \\
            -j "$json"
    """
}

//    . "/home/lwoods/miniconda3/etc/profile.d/conda.sh"
//     conda activate vast-db

process runMSAAF3 {
    queue 'compute'
    cpus '8'
    memory '64GB'
    executor "slurm"
    clusterOptions '--time=4:00:00'
    tag "${proteinType}_${seq}"

    input:
    tuple val(species), val(proteinType), val(seq), val(chain), val(name), val(proteinClass), path(json)

    output:
    tuple val(species), val(proteinType), val(seq), val(chain), val(name), val(proteinClass), path("*/*.json")

    script:
    """
    module load singularity

    singularity exec \\
        -B /home,/scratch,/tgen_labs,/ref_genomes \\
        --cleanenv \\
        /tgen_labs/altin/alphafold3/containers/alphafold_3.0.1.sif \\
        python /app/alphafold/run_alphafold.py \\
            --json_path=$json \\
            --model_dir=/ref_genomes/alphafold/alphafold3/models \\
            --db_dir=/ref_genomes/alphafold/alphafold3/ \\
            --output_dir=. \\
            --norun_inference
    """
}