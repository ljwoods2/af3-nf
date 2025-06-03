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
params.collate = 50
params.compress = false
params.seq_col = "sequence"
params.msa_subdir = "any"
params.job_name_col = "job_name"

params.check_inf_exists = false

include { composeSingleProteinJSON } from './modules/json'
include { batchedAlphaInference; cleanInferenceDir } from './modules/inference'

workflow {

    protein_channel = Channel
        .fromPath(params.input_csv)
        .splitCsv(header: true)
        .map { 
            row ->
                tuple(row[params.job_name_col], 
                      row[params.seq_col],
                      params.msa_subdir, 
                    ) 
            }

    protein_json = composeSingleProteinJSON(protein_channel)

    batched_protein_json = protein_json.map{
        protein_json_tuples -> protein_json_tuples[1]
    }.collate(params.collate)

    batchdir_proc = batchedAlphaInference(batched_protein_json)

    unbatched_inference_dir = batchdir_proc.flatten()

    if (params.compress == true) {
        cleanInferenceDir(unbatched_inference_dir)
    }
}