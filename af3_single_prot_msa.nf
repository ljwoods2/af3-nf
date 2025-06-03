/*
Run AF3 MSA from CSV input
*/

params.input_csv = null
params.msa_db = null
params.no_peptide = false
params.seq_col = "sequence"
params.msa_subdir = "any"

include { filterMissingMSA } from './modules/msa'
include { storeMSA; runMSAAF3 } from './modules/msa'
include { generateSingleJSON } from './modules/json'


def buildChannel(csvFile, seqColumn, msaSubdir) {
    return Channel.fromPath(csvFile)
        .splitCsv(header: true)
        .filter { row ->
            def seq = row[seqColumn]
            seq != null && seq.trim()
        }
        .map { row -> 
            tuple(null, msaSubdir, row[seqColumn], null, null, null)
        }
        .unique()
}

workflow msaWorkflow {
    take:
    input_tuple

    main:
    filt = filterMissingMSA(input_tuple).map { file -> 
            line = file.text.split(",")
            tuple (line[0], line[1], line[2], line[3], line[4], line[5])
        }
    json = generateSingleJSON(filt)
    msa = runMSAAF3(json)
    storeMSA(msa)
}


workflow single_chain {
    take:
    single_chain

    main:
    msaWorkflow(single_chain)
}


workflow {
    single_chain(buildChannel(params.input_csv, params.seq_col, params.msa_subdir))
}
