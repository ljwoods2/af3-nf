process batchedAlphaInference {
    queue 'gpu-a100'
    cpus '8'
    clusterOptions '--nodes=1 --ntasks=1 --gres=gpu:1 --time=24:00:00'
    memory '64GB'
    executor "slurm"
    tag "$batch_name"
    publishDir "${params.out_dir}/inference/", mode: 'copy'

    input:
    tuple val(batch_name), path(batch_dir)

    output:
    tuple val(batch_name), path('*')

    script:
    """
    module load singularity

    singularity exec --nv \\
        -B /home,/scratch,/tgen_labs,/ref_genomes --cleanenv \\
        /tgen_labs/altin/alphafold3/containers/alphafold_3.0.1.sif \\
        python /app/alphafold/run_alphafold.py \\
            --input_dir=$batch_dir \\
            --model_dir=/ref_genomes/alphafold/alphafold3/models \\
            --db_dir=/ref_genomes/alphafold/alphafold3/ \\
            --output_dir=. \\
            --norun_data_pipeline \\
            --num_diffusion_samples=1
        """
}