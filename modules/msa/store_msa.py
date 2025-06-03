import argparse
import json
import os
import vastdb
import pyarrow as pa
from datetime import date
from pathlib import Path
import hashlib
import fcntl
from contextlib import contextmanager

VAST_S3_ACCESS_KEY_ID = os.getenv("VAST_S3_ACCESS_KEY_ID")
VAST_S3_SECRET_ACCESS_KEY = os.getenv("VAST_S3_SECRET_ACCESS_KEY")

EPOCH = date(1970, 1, 1)


# https://gist.github.com/lonetwin/7b4ccc93241958ff6967
@contextmanager
def locked_open(filename, mode="r"):
    """locked_open(filename, mode='r') -> <open file object>

    Context manager that on entry opens the path `filename`, using `mode`
    (default: `r`), and applies an advisory write lock on the file which
    is released when leaving the context. Yields the open file object for
    use within the context.
    Note: advisory locking implies that all calls to open the file using
    this same api will block for both read and write until the lock is
    acquired. Locking this way will not prevent the file from access using
    any other api/method.
    """
    if mode in ("r", "rb"):
        lock = fcntl.LOCK_SH
    else:
        lock = fcntl.LOCK_EX

    with open(filename, mode) as fd:
        fcntl.flock(fd, lock)
        yield fd
        fcntl.flock(fd, fcntl.LOCK_UN)


def read_json(json_path, seq):
    """Reads the JSON file and extracts relevant data."""
    try:
        with open(json_path, "r") as f:
            data = json.load(f)

        msa_data = data.get("sequences", None)[0].get("protein", None)
        del msa_data["id"]

        empty_query = f">query\n{seq}\n"

        if (
            msa_data["unpairedMsa"] == empty_query
            and msa_data["pairedMsa"] == empty_query
        ):
            is_empty = True
        else:
            is_empty = False

        return json.dumps(msa_data), is_empty
    except Exception as e:
        print(f"Error processing JSON: {e}")
        return None


def store_in_database(
    species,
    protein_type,
    seq,
    db_url,
    msa_json,
    is_empty,
    name=None,
    chain=None,
    protein_class=None,
):
    """Stores the JSON directly into the VAST database."""

    date_val = (date.today() - EPOCH).days

    h = hashlib.sha256()

    if protein_type == "peptide":
        fname = seq + ".json"
    else:
        h.update(seq.encode("utf-8"))
        fname = h.hexdigest() + ".json"

    filepath = Path("/tgen_labs/altin/alphafold3/msa") / protein_type / fname

    with locked_open(filepath, "w+") as f:
        try:
            session = vastdb.connect(
                endpoint=db_url,
                access=VAST_S3_ACCESS_KEY_ID,
                secret=VAST_S3_SECRET_ACCESS_KEY,
                ssl_verify=False,
            )

            # manually perform an UPSERT
            with session.transaction() as tx:
                bucket = tx.bucket("altindbs3")
                schema = bucket.schema("alphafold-3")

                if protein_type == "tcr":
                    table = schema.table("tcr_chain_msa")
                    primary_key_name = "tcr_chain_msa_id"
                    predicate = table["tcr_chain_msa_id"] == seq

                    data = [
                        [seq],
                        [chain],
                        [filepath.as_posix()],
                        [is_empty],
                        [date_val],
                    ]

                    new_row = pa.table(schema=table.arrow_schema, data=data)

                elif protein_type == "mhc":
                    table = schema.table("mhc_chain_msa")
                    primary_key_name = "mhc_chain_msa_id"
                    predicate = (table["mhc_chain_msa_id"] == seq) & (
                        table["species"] == species
                    )

                    data = [
                        [seq],
                        [name],
                        [chain],
                        [protein_class],
                        [species],
                        [filepath.as_posix()],
                        [is_empty],
                        [date_val],
                    ]
                    new_row = pa.table(schema=table.arrow_schema, data=data)

                elif protein_type == "peptide":
                    table = schema.table("peptide_msa")
                    primary_key_name = "peptide_msa_id"
                    predicate = table["peptide_msa_id"] == seq
                    data = [
                        [seq],
                        [filepath.as_posix()],
                        [is_empty],
                        [date_val],
                    ]
                    new_row = pa.table(
                        schema=table.arrow_schema,
                        data=data,
                    )
                elif protein_type == "any":
                    table = schema.table("any_msa")
                    primary_key_name = "any_msa_id"
                    predicate = table["any_msa_id"] == seq
                    data = [
                        [seq],
                        [filepath.as_posix()],
                        [is_empty],
                        [date_val],
                    ]
                    new_row = pa.table(
                        schema=table.arrow_schema,
                        data=data,
                    )
                else:
                    raise ValueError

                # first SELECT
                result = table.select(
                    columns=[primary_key_name],
                    predicate=predicate,
                    internal_row_id=True,
                ).read_all()

                # either insert or update
                if result.shape[0] == 0:
                    table.insert(new_row)
                else:

                    schema = pa.schema(
                        [pa.field("$row_id", pa.uint64())]
                        + list(table.arrow_schema)
                    )
                    data = [[result["$row_id"][0]]] + data
                    updated_row = pa.table(schema=schema, data=data)
                    table.update(updated_row)

            f.write(msa_json)
            f.flush()

        except Exception as e:
            raise ConnectionError(f"Error connecting to database: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Store MSA JSON into SQLite DB."
    )

    parser.add_argument(
        "-sp", "--species", type=str, required=True, help="Species"
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
        "-j",
        "--json_msa_path",
        type=str,
        required=True,
        help="Path to JSON MSA",
    )

    parser.add_argument(
        "-db",
        "--database",
        type=str,
        required=True,
        help="VAST database URL",
    )

    args = parser.parse_args()

    if not os.path.exists(args.json_msa_path):
        print(f"Error: JSON file {args.json_msa_path} not found.")
        exit(1)

    msa_json, is_empty = read_json(args.json_msa_path, args.seq)

    store_in_database(
        args.species,
        args.protein_type,
        args.seq,
        args.database,
        msa_json,
        is_empty,
        chain=args.chain,
        name=args.name,
        protein_class=args.protein_class,
    )
