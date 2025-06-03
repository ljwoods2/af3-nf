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
params.check_inf_exists = false

include { composePMHCJSON } from './modules/json'
include { batchedAlphaInference; cleanInferenceDir } from './modules/inference'

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

    batched_triad_json = triad_json.map{
        triad_json_tuples -> triad_json_tuples[1]
    }.collate(params.collate)

    // batchname_batchdir = batched_triad_json.flatMap { batch_list ->
        
    //     def jsonFiles = batch_list.collect { tuple -> tuple[1] }
        
    //     // def uniqueDirName = "batch_${UUID.randomUUID().toString()}/"
        
    //     Channel.from(jsonFiles).collectFile()
    // }.map { batchDir ->
    //     tuple(batchDir.getName(), batchDir.getTempDir())
    // }

    batchdir_proc = batchedAlphaInference(batched_triad_json)

    unbatched_inference_dir = batchdir_proc.flatten()

    if (params.compress == true) {
        cleanInferenceDir(unbatched_inference_dir)
    }
}