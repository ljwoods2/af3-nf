/*
Run AF3 MSA from CSV input
*/

params.input_csv = null
params.msa_db = null
params.no_peptide = false

include { filterMissingMSA } from './modules/msa'

include { storeMSA; runMSAAF3 } from './modules/msa'

include { generateSingleJSON } from './modules/json'


def buildChannel(csvFile, proteinType, seqColumn, chainColumn, speciesColumn, nameColumn, classColumn) {
    return Channel.fromPath(csvFile)
        .splitCsv(header: true)
        .filter { row ->
            def seq = row[seqColumn]
            seq != null && seq.trim()
        }
        .map { row -> 
            if (proteinType == "tcr") {
                tuple(row[speciesColumn], proteinType, row[seqColumn], row[chainColumn], null, null)
            }
            else if (proteinType == "mhc") {
                tuple(row[speciesColumn], proteinType, row[seqColumn], row[chainColumn], row[nameColumn], row[classColumn])
            }
            else {
                tuple(null, proteinType, row[seqColumn], null, null, null)
            }
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
    tcr1(buildChannel(params.input_csv, "tcr", "tcr_1_seq", "tcr_1_chain", "tcr_1_species", null, null))
    tcr2(buildChannel(params.input_csv, "tcr", "tcr_2_seq", "tcr_2_chain", "tcr_2_species", null, null))
    mhc1(buildChannel(params.input_csv, "mhc", "mhc_1_seq", "mhc_1_chain", "mhc_1_species", "mhc_1_name",  "mhc_class"))
    mhc2(buildChannel(params.input_csv, "mhc", "mhc_2_seq", "mhc_2_chain", "mhc_2_species", "mhc_2_name",  "mhc_class"))

    if (params.no_peptide == false) {
        peptide(buildChannel(params.input_csv, "peptide", "peptide", null, null, null, null))
    }
}
