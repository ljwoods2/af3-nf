import argparse
import sqlite3


def check_database(name, seq, db_url, table_name):
    """Checks if the name exists in the SQLite database."""
    try:

        conn = sqlite3.connect(db_url)
        cursor = conn.cursor()

        result = cursor.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE name = ?", (name,)
        )
        result = result.fetchone()[0]

        # NF compatible tuple.
        if result == 0:
            print(f"{name},{seq},{table_name}")

        conn.close()
    except sqlite3.Error as e:
        raise ConnectionError(f"Error connecting to database: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check if an MSA entry is missing from SQLite DB."
    )

    parser.add_argument(
        "-n", "--name", type=str, required=True, help="Protein name"
    )
    parser.add_argument(
        "-s", "--seq", type=str, required=True, help="Protein sequence"
    )
    parser.add_argument(
        "-db",
        "--database",
        type=str,
        required=True,
        help="SQLite database URL",
    )
    parser.add_argument(
        "-t", "--table", type=str, required=True, help="Table name in database"
    )

    args = parser.parse_args()

    check_database(args.name, args.seq, args.database, args.table)
