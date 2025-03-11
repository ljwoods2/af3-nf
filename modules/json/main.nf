process generateSingleJSON {
    queue 'compute'
    executor "slurm"
    tag "${proteinType}_${seq}"

    input:
    tuple val(species), val(proteinType), val(seq), val(chain), val(name), val(proteinClass)

    output:
    tuple val(species), val(proteinType), val(seq), val(chain), val(name), val(proteinClass), path("*.json")

    script:
    """
    module load singularity

    fname=\$(uuidgen)

    singularity exec --nv \\
        -B /home,/scratch,/tgen_labs --cleanenv \\
        /tgen_labs/altin/alphafold3/containers/msa-db.sif \\
        python ${workflow.projectDir}/modules/json/generate_single_JSON.py \\
            -s "$seq" \\
            -jn "\$fname" 
    """
 }

     // module load Python/3.7.4-GCCcore-8.3.0

    // fname=\$(uuidgen)

    // python ${workflow.projectDir}/modules/json/generate_single_JSON.py \\
    //     -s "$seq" \\
    //     -jn "\$fname" 




process composeTriadJSON {
    queue 'compute'
    executor "slurm"
    tag "$job_name"
    // publishDir "${params.out_dir}/inference_input", mode: 'copy'

    input:
    tuple   val(job_name), 
            val(peptide),
            val(mhc_1_seq), 
            val(mhc_2_seq), 
            val(tcr_1_seq), 
            val(tcr_2_seq)

    output:
    tuple val(job_name), path("*.json")

    script:
    def peptide_msa = params.no_peptide ? '' : "-pm"
    def seeds = params.seeds ? "--seeds ${params.seeds}" : ''
    """
    module load singularity

    export SINGULARITYENV_VAST_S3_ACCESS_KEY_ID="\$VAST_S3_ACCESS_KEY_ID"
    export SINGULARITYENV_VAST_S3_SECRET_ACCESS_KEY="\$VAST_S3_SECRET_ACCESS_KEY"

    singularity exec \\
        -B /home,/scratch,/tgen_labs --cleanenv \\
        /tgen_labs/altin/alphafold3/containers/msa-db.sif \\
        python ${workflow.projectDir}/modules/json/compose_inference_JSON.py \\
            -jn "$job_name" \\
            -p "$peptide" \\
            ${peptide_msa} \\
            -m1s "$mhc_1_seq" \\
            -m2s "$mhc_2_seq" \\
            -t1s "$tcr_1_seq" \\
            -t2s "$tcr_2_seq" \\
            ${seeds} \\
            -db "$params.msa_db"
    """
 }

    //  . "/home/lwoods/miniconda3/etc/profile.d/conda.sh"
    // conda activate vast-db


process composePMHCJSON {
    queue 'compute'
    executor "slurm"
    tag "$job_name"
    // publishDir "${params.out_dir}/inference_input", mode: 'copy'

    input:
    tuple   val(job_name), 
            val(peptide),
            val(mhc_1_seq), 
            val(mhc_2_seq)

    output:
    tuple val(job_name), path("*.json")

    script:
    def peptide_msa = params.no_peptide ? '' : "-pm"
    def seeds = params.seeds ? "--seeds ${params.seeds}" : ''
    """
    module load singularity

    export SINGULARITYENV_VAST_S3_ACCESS_KEY_ID="\$VAST_S3_ACCESS_KEY_ID"
    export SINGULARITYENV_VAST_S3_SECRET_ACCESS_KEY="\$VAST_S3_SECRET_ACCESS_KEY"

    singularity exec \\
        -B /home,/scratch,/tgen_labs --cleanenv \\
        /tgen_labs/altin/alphafold3/containers/msa-db.sif \\
        python ${workflow.projectDir}/modules/json/compose_inference_JSON.py \\
            -jn "$job_name" \\
            -p "$peptide" \\
            ${peptide_msa} \\
            -m1s "$mhc_1_seq" \\
            -m2s "$mhc_2_seq" \\
            ${seeds} \\
            -db "$params.msa_db"
    """
 }