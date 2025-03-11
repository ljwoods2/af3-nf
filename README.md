

## Usage

Modify your nextflow config to include VAST database keys

```bash
vim nextflow.config
```

```javascript
env {
    VAST_S3_ACCESS_KEY_ID = "<id>"
    VAST_S_SECRET_ACCESS_KEY = "<key>"
}
```

To run, make a job CSV in the format described [here](https://github.com/ljwoods2/tcr_format_parsers)

```bash
module load Java/11.0.2

nextflow run \
    -w /work/path \
    -c /path/to/nextflow.config \
    af3_triad_msa.nf \
        --input_csv '/path/to/formatted/csv' \
        --out_dir '.' \
        --msa_db '<vast_url>' && \
nextflow run \
    -w /work/path \
    -c /path/to/nextflow.config \
    af3_triad_inference.nf \
        --input_csv '/path/to/formatted/csv' \
        --out_dir '.' \
        --msa_db '<vast_url>' \
        --seeds 1,2,3,4,5
```

Optionally pass `--no_peptide` to both pipelines to run inference without peptide MSAs.