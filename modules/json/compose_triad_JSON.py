#!/usr/bin/env python3
import argparse
import sqlite3
import json
import sys


def get_msa(conn, table, name):
    """
    Query the specified table for the msa JSON corresponding to the given name.
    Returns the parsed JSON object if found, otherwise None.
    """
    cur = conn.cursor()
    query = f"SELECT msa FROM {table} WHERE name = ?"
    cur.execute(query, (name,))
    row = cur.fetchone()
    if row:
        try:
            # Assume the msa field is stored as valid JSON text
            return json.loads(row[0])
        except json.JSONDecodeError:
            sys.stderr.write(
                f"Warning: Failed to decode JSON for {name} in table {table}\n"
            )
            return None
    else:
        sys.stderr.write(f"Warning: {name} not found in table {table}\n")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Compose Alphafold3 input JSON by querying SQLite for chain MSA information."
    )
    parser.add_argument(
        "-jn", "--job_name", type=str, required=True, help="Job name"
    )
    parser.add_argument(
        "-p", "--peptide", type=str, required=True, help="Peptide sequence"
    )
    parser.add_argument(
        "-pm",
        "--peptide_msa",
        action="store_true",
        help="Use peptide MSA",
    )
    parser.add_argument(
        "-m1t",
        "--mhc_1_type",
        type=str,
        required=True,
        help="MHC 1 type (used as table name in DB)",
    )
    parser.add_argument(
        "-m1n", "--mhc_1_name", type=str, required=True, help="MHC 1 name"
    )
    parser.add_argument(
        "-m1s", "--mhc_1_seq", type=str, required=True, help="MHC 1 sequence"
    )
    parser.add_argument(
        "-m2t",
        "--mhc_2_type",
        type=str,
        required=True,
        help="MHC 2 type (used as table name in DB)",
    )
    parser.add_argument(
        "-m2n", "--mhc_2_name", type=str, required=True, help="MHC 2 name"
    )
    parser.add_argument(
        "-m2s", "--mhc_2_seq", type=str, required=True, help="MHC 2 sequence"
    )
    parser.add_argument(
        "-t1t", "--tcr_1_type", type=str, required=True, help="TCR 1 type"
    )
    parser.add_argument(
        "-t1n", "--tcr_1_name", type=str, required=True, help="TCR 1 name"
    )
    parser.add_argument(
        "-t1s", "--tcr_1_seq", type=str, required=True, help="TCR 1 sequence"
    )
    parser.add_argument(
        "-t2t", "--tcr_2_type", type=str, required=True, help="TCR 2 type"
    )
    parser.add_argument(
        "-t2n", "--tcr_2_name", type=str, required=True, help="TCR 2 name"
    )
    parser.add_argument(
        "-t2s", "--tcr_2_seq", type=str, required=True, help="TCR 2 sequence"
    )
    parser.add_argument(
        "-db",
        "--database",
        type=str,
        required=True,
        help="SQLite database URL/path",
    )
    args = parser.parse_args()

    try:
        conn = sqlite3.connect(args.database)
    except sqlite3.Error as e:
        sys.stderr.write(f"Error connecting to database: {e}\n")
        sys.exit(1)

    if args.peptide_msa:
        peptide_msa = get_msa(conn, "peptide", args.peptide)
        peptide_msa["protein"].update({"id": "A"})
    else:
        peptide_msa = {
            "protein": {
                "id": "A",
                "sequence": args.peptide,
                "unpairedMsa": f">query\\n{args.peptide}\\n",
                "pairedMsa": f">query\\n{args.peptide}\\n",
                "templates": [],
            },
        }
    mhc_1_msa = get_msa(conn, args.mhc_1_type, args.mhc_1_name)
    mhc_1_msa["protein"].update({"id": "B"})
    mhc_2_msa = get_msa(conn, args.mhc_2_type, args.mhc_2_name)
    mhc_2_msa["protein"].update({"id": "C"})
    tcr_1_msa = get_msa(conn, args.tcr_1_type, args.tcr_1_name)
    tcr_1_msa["protein"].update({"id": "D"})
    tcr_2_msa = get_msa(conn, args.tcr_2_type, args.tcr_2_name)
    tcr_2_msa["protein"].update({"id": "E"})
    conn.close()

    final_json = {
        "name": args.job_name,
        "modelSeeds": [42],
        "sequences": [
            peptide_msa,
            mhc_1_msa,
            mhc_2_msa,
            tcr_1_msa,
            tcr_2_msa,
        ],
        "dialect": "alphafold3",
        "version": 1,
    }
    with open(args.job_name + ".json", "w") as f:
        json.dump(final_json, f, indent=2)


if __name__ == "__main__":
    main()
