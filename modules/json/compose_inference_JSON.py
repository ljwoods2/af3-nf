#!/usr/bin/env python3
import argparse
import sqlite3
import json
import sys
import os
import vastdb
import fcntl
from contextlib import contextmanager

VAST_S3_ACCESS_KEY_ID = os.getenv("VAST_S3_ACCESS_KEY_ID")
VAST_S3_SECRET_ACCESS_KEY = os.getenv("VAST_S3_SECRET_ACCESS_KEY")


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


def get_msa(session, protein_type, seq, species=None):
    """
    Query the specified table for the msa JSON corresponding to the given name.
    Returns the parsed JSON object if found, otherwise None.
    """

    with session.transaction() as tx:
        bucket = tx.bucket("altindbs3")
        schema = bucket.schema("alphafold-3")

        if protein_type == "tcr":
            table = schema.table("tcr_chain_msa")
            predicate = table["tcr_chain_msa_id"] == seq

        elif protein_type == "mhc":
            table = schema.table("mhc_chain_msa")
            predicate = table["mhc_chain_msa_id"] == seq

        elif protein_type == "peptide":
            table = schema.table("peptide_msa")
            predicate = table["peptide_msa_id"] == seq

        elif protein_type == "any":
            table = schema.table("any_msa")
            predicate = table["any_msa_id"] == seq

        else:
            raise ValueError

        result = table.select(
            columns=["msa_path"], predicate=predicate
        ).read_all()

        if result.shape[0] != 1:
            raise ValueError(
                f"Error fetching MSA for {protein_type} {seq}. "
                f"Expected 1 row, got {result.shape[0]}"
            )

        # if currently being rewritten, wait to avoid
        # reading incomplete data
        with locked_open(result["msa_path"][0].as_py(), "r") as f:
            msa = json.load(f)

        return msa


def main():
    parser = argparse.ArgumentParser(
        description="Compose Alphafold3 input JSON by querying VAST for chain MSA information."
    )
    parser.add_argument(
        "-jn", "--job_name", type=str, required=True, help="Job name"
    )
    parser.add_argument(
        "-p", "--peptide", type=str, required=False, help="Peptide sequence"
    )
    parser.add_argument(
        "-pm",
        "--peptide_msa",
        action="store_true",
        help="Use peptide MSA",
    )
    parser.add_argument(
        "-pt",
        "--protein_type",
        type=str,
        required=False,
        help="Protein type (MSA subdir) if using script for a single chain",
    )
    parser.add_argument(
        "-m1s", "--mhc_1_seq", type=str, required=False, help="MHC 1 sequence"
    )
    parser.add_argument(
        "-m2s", "--mhc_2_seq", type=str, required=False, help="MHC 2 sequence"
    )
    parser.add_argument(
        "-t1s", "--tcr_1_seq", type=str, required=False, help="TCR 1 sequence"
    )
    parser.add_argument(
        "-t2s", "--tcr_2_seq", type=str, required=False, help="TCR 2 sequence"
    )
    parser.add_argument(
        "-s",
        "--seeds",
        type=str,
        required=False,
        default="42",
        help="Comma separated list of model seeds",
    )
    parser.add_argument(
        "-db",
        "--database",
        type=str,
        required=True,
        help="SQLite database URL/path",
    )
    args = parser.parse_args()

    seeds = [int(seed) for seed in args.seeds.split(",")]

    session = vastdb.connect(
        endpoint=args.database,
        access=VAST_S3_ACCESS_KEY_ID,
        secret=VAST_S3_SECRET_ACCESS_KEY,
        ssl_verify=False,
    )
    msas = []
    if args.peptide:
        if args.peptide_msa:
            ptype = "peptide" if args.protein_type else args.protein_type
            peptide_msa = get_msa(session, ptype, args.peptide)
            peptide_msa["id"] = "A"
            peptide_msa = {"protein": peptide_msa}
            # peptide_msa["protein"].update({"id": "A"})
        else:
            peptide_msa = {
                "protein": {
                    "id": "A",
                    "sequence": args.peptide,
                    "unpairedMsa": f">query\n{args.peptide}\n",
                    "pairedMsa": f">query\n{args.peptide}\n",
                    "templates": [],
                },
            }
        msas.append(peptide_msa)

    if args.mhc_1_seq:
        mhc_1_msa = get_msa(session, "mhc", args.mhc_1_seq)
        # mhc_1_msa["protein"].update({"id": "B"})
        mhc_1_msa["id"] = "B"
        mhc_1_msa = {"protein": mhc_1_msa}
        msas.append(mhc_1_msa)

    if args.mhc_2_seq:
        mhc_2_msa = get_msa(session, "mhc", args.mhc_2_seq)
        mhc_2_msa["id"] = "C"
        mhc_2_msa = {"protein": mhc_2_msa}
        msas.append(mhc_2_msa)

    if args.tcr_1_seq:
        tcr_1_msa = get_msa(session, "tcr", args.tcr_1_seq)
        tcr_1_msa["id"] = "D"
        tcr_1_msa = {"protein": tcr_1_msa}
        msas.append(tcr_1_msa)

    if args.tcr_2_seq:
        tcr_2_msa = get_msa(session, "tcr", args.tcr_2_seq)
        tcr_2_msa["id"] = "E"
        tcr_2_msa = {"protein": tcr_2_msa}
        msas.append(tcr_2_msa)
    # # mhc_2_msa["protein"].update({"id": "C"})
    # tcr_1_msa = get_msa(session, "tcr", args.tcr_1_seq)
    # tcr_1_msa["id"] = "D"
    # # tcr_1_msa["protein"].update({"id": "D"})
    # tcr_2_msa = get_msa(session, "tcr", args.tcr_2_seq)
    # tcr_2_msa["id"] = "E"
    # tcr_2_msa["protein"].update({"id": "E"})
    final_json = {
        "name": args.job_name,
        "modelSeeds": seeds,
        "sequences": msas,
        "dialect": "alphafold3",
        "version": 1,
    }
    with open(args.job_name + ".json", "w") as f:
        json.dump(final_json, f, indent=2)


if __name__ == "__main__":
    main()
