/*
Created on 12/8/24
Last Modified on 01/16/25
@author: Lawson Woods
*/
params.input_csv = null
params.out_dir = null
params.msa_db = null
params.no_peptide = false
params.seeds = null

include { composePMHCJSON } from './modules/json'
include { batchedAlphaInference } from './modules/inference'


workflow {

    triad_channel = Channel
        .fromPath(params.input_csv)
        .splitCsv(header: true)
        .map { 
            row ->
                tuple(row.job_name, 
                    row.peptide, 
                    row.mhc_1_seq, 
                    row.mhc_2_seq) 
            }

    triad_json = composePMHCJSON(triad_channel)

    batched_triad_json = triad_json.collate(50)

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