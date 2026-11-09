"""Izvrsava sve upite iz sql/analytics.sql i stampa rezultate."""
import re
from pathlib import Path

import pandas as pd

from .model import connect

SQL_FILE = Path(__file__).resolve().parent.parent / "sql" / "analytics.sql"


def main() -> None:
    text = SQL_FILE.read_text(encoding="utf-8")
    blocks = re.split(r"-- name: ", text)[1:]
    conn = connect()
    pd.set_option("display.width", 140)
    for block in blocks:
        name, _, sql = block.partition("\n")
        print(f"\n=== {name} ===")
        print(pd.read_sql_query(sql, conn).to_string(index=False))
    conn.close()


if __name__ == "__main__":
    main()
