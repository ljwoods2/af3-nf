


process filterMissingMSA {
    queue 'compute'
    executor "slurm"
    tag "${type}_${name}"
    debug true

    input:
    tuple val(name), val(seq), val(type)

    output:
    path "${name}.csv", optional: true

    script:
    """
    python ${workflow.projectDir}/modules/msa/filter_missing_msa.py \\
        -n "$name" \\
        -s "$seq" \\
        -db "$params.msa_db" \\
        -t "$type" > "${name}.csv"
    """
}

process storeMSA {
    queue 'compute'
    executor "slurm"
    tag "${type}_${name}"
    
    input:
    tuple val(name), val(seq), val(type), path(json)

    script:
    """
    module load Python/3.7.4-GCCcore-8.3.0

    python ${workflow.projectDir}/modules/msa/store_msa.py \\
        -n "$name" \\
        -s "$seq" \\
        -i "$json" \\
        -db "$params.msa_db" \\
        -t "$type"
    """
}

process runMSAAF3 {
    queue 'compute'
    cpus '8'
    memory '64GB'
    executor "slurm"
    clusterOptions '--time=4:00:00'
    tag "${type}_${name}"

    input:
    tuple val(name), val(seq), val(type), path(json)

    output:
    tuple val(name), val(seq), val(type), path("*/*.json")

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