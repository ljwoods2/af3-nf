/*
Run AF3 MSA from CSV input
*/
include { filterMissingMSA } from './modules/msa'

include { storeMSA; runMSAAF3 } from './modules/msa'

include { generateSingleJSON } from './modules/json'

params.input_csv = null
params.out_dir = null
params.msa_db = null
params.no_peptide = false

def buildChannel(csvFile, nameColumn, seqColumn, typeLabel) {
    return Channel.fromPath(csvFile)
        .splitCsv(header: true)
        .map { row -> 
            row[nameColumn] ? tuple(row[nameColumn], row[seqColumn], (typeLabel == "peptide" ? "peptide" : row[typeLabel])) : null
        }
        .filter { it != null }
        .distinct()
}

workflow msaWorkflow {
    take:
    input_tuple

    main:
    filt = filterMissingMSA(input_tuple).map { file -> 
        def line = file.text.trim().split(",")
        line.size() == 3 ? tuple (line[0], line[1], line[2]) : null
        }
        .filter { it != null }
    json = generateSingleJSON(filt)
    msa = runMSAAF3(json)
    storeMSA(msa)
}

workflow tcr1 {
    take:
    tcr_1

    main:
    msaWorkflow(tcr_1)
}

workflow tcr2 {
    take:
    tcr_2

    main:
    msaWorkflow(tcr_2)
}

workflow mhc1 {
    take:
    mhc_1

    main:
    msaWorkflow(mhc_1)
}

workflow mhc2 {
    take:
    mhc_2

    main:
    msaWorkflow(mhc_2)
}

workflow peptide {
    take:
    peptide

    main:
    msaWorkflow(peptide)
}


workflow {
    tcr1(buildChannel(params.input_csv, "tcr_1_name", "tcr_1_seq", "tcr_1_type"))
    tcr2(buildChannel(params.input_csv, "tcr_2_name", "tcr_2_seq", "tcr_2_type"))
    mhc1(buildChannel(params.input_csv, "mhc_1_name", "mhc_1_seq", "mhc_1_type"))
    mhc2(buildChannel(params.input_csv, "mhc_2_name", "mhc_2_seq", "mhc_2_type"))

    if (params.no_peptide == false) {
        peptide(buildChannel(params.input_csv, "peptide", "peptide", "peptide"))
    }
}
