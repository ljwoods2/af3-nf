import argparse
import sqlite3
import json
import os
import time


def read_json(json_path):
    """Reads the JSON file and extracts relevant data."""
    try:
        with open(json_path, "r") as f:
            data = json.load(f)

        msa_data = data.get("sequences", None)
        if msa_data is None:
            raise ValueError(f"Key 'msa_data' not found in {json_path}")

        return json.dumps(msa_data[0])
    except Exception as e:
        print(f"Error processing JSON: {e}")
        return None


def store_in_database(name, seq, msa_json, db_path, table_name):
    """Stores the JSON directly into the SQLite database."""

    if table_name == "peptide":
        sql = f"""
            INSERT INTO {table_name} (name, msa)
            VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET msa=excluded.msa
        """
        tpl = (name, msa_json)
    else:
        sql = f"""
            INSERT INTO {table_name} (name, seq, msa)
            VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET msa=excluded.msa
        """
        tpl = (name, seq, msa_json)

        # try:
        #     conn = sqlite3.connect(db_path)
        #     cursor = conn.cursor()

        #     cursor.execute(
        #         sql,
        #         tpl,
        #     )

        #     conn.commit()
        #     conn.close()
        #     print(f"Stored {name} in {table_name} table.")
        # except sqlite3.Error as e:
        #     print(f"Database error: {e}")
    retries = 5
    delay = 1.0  # start with a 1-second delay
    for attempt in range(retries):
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            cursor.execute(sql, tpl)
            conn.commit()
            conn.close()
            print(f"Stored {name} in {table_name} table.")
            break  # successful write, exit the loop
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                print(
                    f"Database is locked. Retry {attempt+1}/{retries} in {delay} seconds."
                )
                time.sleep(delay)
                delay *= 2  # exponential backoff
                continue
            else:
                print(f"Operational error: {e}")
                break
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            break
    else:
        print("Failed to store in database after several retries.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Store MSA JSON into SQLite DB."
    )

    parser.add_argument(
        "-n", "--name", type=str, required=True, help="Protein name"
    )
    parser.add_argument(
        "-s", "--seq", type=str, required=True, help="Protein sequence"
    )
    parser.add_argument(
        "-i", "--json", type=str, required=True, help="Input JSON file"
    )
    parser.add_argument(
        "-db",
        "--database",
        type=str,
        required=True,
        help="SQLite database path",
    )
    parser.add_argument(
        "-t", "--table", type=str, required=True, help="Table name"
    )

    args = parser.parse_args()

    if not os.path.exists(args.json):
        print(f"Error: JSON file {args.json} not found.")
        exit(1)

    msa_json = read_json(args.json)
    if msa_json:
        store_in_database(
            args.name, args.seq, msa_json, args.database, args.table
        )
