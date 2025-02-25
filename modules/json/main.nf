process generateSingleJSON {
    queue 'compute'
    executor "slurm"
    tag "${type}_${name}"

    input:
    tuple val(name), val(seq), val(type)

    output:
    tuple val(name), val(seq), val(type), path("*.json")

    script:
    """
    module load Python/3.7.4-GCCcore-8.3.0

    python ${workflow.projectDir}/modules/json/generate_single_JSON.py \\
        -n "$name" \\
        -s "$seq" \\
        -jn "${type}_${name}" 
    """
 }

process composeTriadJSON {
    queue 'compute'
    executor "slurm"
    tag "$job_name"
    // publishDir "${params.out_dir}/inference_input", mode: 'copy'

    input:
    tuple   val(job_name), val(peptide),
            val(mhc_1_type), val(mhc_1_name), val(mhc_1_seq), 
            val(mhc_2_type), val(mhc_2_name), val(mhc_2_seq), 
            val(tcr_1_type), val(tcr_1_name), val(tcr_1_seq), 
            val(tcr_2_type), val(tcr_2_name), val(tcr_2_seq)

    output:
    tuple val(job_name), path("*.json")

    script:
    def peptide_msa = params.no_peptide ? '' : "-pm"
    """
    module load Python/3.7.4-GCCcore-8.3.0

    python ${workflow.projectDir}/modules/json/compose_triad_JSON.py \\
        -jn "$job_name" \\
        -p "$peptide" \\
        ${peptide_msa} \\
        -m1t "$mhc_1_type" \\
        -m1n "$mhc_1_name" \\
        -m1s "$mhc_1_seq" \\
        -m2t "$mhc_2_type" \\
        -m2n "$mhc_2_name" \\
        -m2s "$mhc_2_seq" \\
        -t1t "$tcr_1_type" \\
        -t1n "$tcr_1_name" \\
        -t1s "$tcr_1_seq" \\
        -t2t "$tcr_2_type" \\
        -t2n "$tcr_2_name" \\
        -t2s "$tcr_2_seq" \\
        -db "$params.msa_db"
    """
 }