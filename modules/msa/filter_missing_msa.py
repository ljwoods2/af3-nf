import argparse
import vastdb
import os


VAST_S3_ACCESS_KEY_ID = os.getenv("VAST_S3_ACCESS_KEY_ID")
VAST_S3_SECRET_ACCESS_KEY = os.getenv("VAST_S3_SECRET_ACCESS_KEY")


def is_msa_stored(protein_type, seq, species, db_url):
    """Checks if the name exists in the VAST database."""
    try:

        session = vastdb.connect(
            endpoint=db_url,
            access=VAST_S3_ACCESS_KEY_ID,
            secret=VAST_S3_SECRET_ACCESS_KEY,
            ssl_verify=False,
        )

        with session.transaction() as tx:

            bucket = tx.bucket("altindbs3")
            schema = bucket.schema("alphafold-3")

            if protein_type == "tcr":
                table = schema.table("tcr_chain_msa")
                predicate = table["tcr_chain_msa_id"] == seq
                primary_key_name = "tcr_chain_msa_id"

            elif protein_type == "mhc":
                table = schema.table("mhc_chain_msa")
                predicate = (table["mhc_chain_msa_id"] == seq) & (
                    table["species"] == species
                )
                primary_key_name = "mhc_chain_msa_id"
            elif protein_type == "peptide":
                table = schema.table("peptide_msa")
                predicate = table["peptide_msa_id"] == seq
                primary_key_name = "peptide_msa_id"
            else:
                raise ValueError

            result = table.select(
                columns=[primary_key_name], predicate=predicate
            ).read_all()

            if result.shape[0] == 0:
                return False
            return True
    except Exception as e:
        raise ConnectionError(f"Error connecting to database: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check if an MSA entry is missing from SQLite DB."
    )
    parser.add_argument(
        "-sp", "--species", type=str, required=False, help="Species"
    )
    parser.add_argument(
        "-t", "--protein_type", type=str, required=True, help="Protein type"
    )
    parser.add_argument(
        "-s", "--seq", type=str, required=True, help="Protein sequence"
    )
    parser.add_argument(
        "-c", "--chain", type=str, required=False, help="Protein chain type"
    )
    parser.add_argument(
        "-n", "--name", type=str, required=False, help="Protein name"
    )
    parser.add_argument(
        "-cl",
        "--protein_class",
        type=str,
        required=False,
        help="Protein class",
    )

    parser.add_argument(
        "-db",
        "--database",
        type=str,
        required=True,
        help="VAST database URL",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Output file",
    )
    args = parser.parse_args()

    if not is_msa_stored(
        args.protein_type, args.seq, args.species, args.database
    ):
        species = args.species if args.species else "null"
        name = args.name if args.name else "null"
        chain = args.chain if args.chain else "null"
        protein_class = args.protein_class if args.protein_class else "null"

        with open(args.output, "a") as f:
            f.write(
                f"{args.species},{args.protein_type},{args.seq},{chain},{name},{protein_class}"
            )
