/*
Created on 12/8/24
Last Modified on 01/16/25
@author: Lawson Woods

Run AF3 inference with fixed TCR alpha and beta sequences but vary peptide and HLA alpha and beta sequences.
*/
include { composeTriadJSON } from './modules/json'
include { batchedAlphaInference } from './modules/inference'

params.input_csv = null
params.out_dir = null
params.msa_db = null
params.no_peptide = false

workflow {

    triad_channel = Channel
        .fromPath(params.input_csv)
        .splitCsv(header: true)
        .map { 
            row ->
                // def job_name = "${row.hla_a_name}_${row.hla_b_name}_${row.peptide}"
                tuple(row.job_name, row.peptide, 
                    row.mhc_1_type, row.mhc_1_name, row.mhc_1_seq, 
                    row.mhc_2_type, row.mhc_2_name, row.mhc_2_seq, 
                    row.tcr_1_type, row.tcr_1_name, row.tcr_1_seq, 
                    row.tcr_2_type, row.tcr_2_name, row.tcr_2_seq) 
            }

    triad_json = composeTriadJSON(triad_channel)

    batched_triad_json = triad_json.collate(100)

    batchname_batchdir = batched_triad_json.map { batch_list ->

        def batch_dir = file("${workflow.workDir}/batch_${UUID.randomUUID().toString()}")
        batch_dir.mkdirs()

        batch_list.each { tuple ->
            def (job_name, json_path) = tuple
            json_path.copyTo(batch_dir)
        }
        tuple(batch_dir.getName(), batch_dir)
    }
    batchedAlphaInference(batchname_batchdir)
}