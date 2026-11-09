"""GOLD sloj: star schema u SQL skladistu (SQLite lokalno; Synapse/Redshift/BigQuery u cloud-u).

- dim_customer : SCD tip 2 (istorija promene grada)
- dim_account  : upsert (tip 1)
- dim_date     : kalendarska dimenzija
- fact_transactions : upsert po txn_id (idempotentno), customer_key se uzima
                      'as-of' datum transakcije
"""
import sqlite3

import pandas as pd

from .common import PARTITION, db_path, get_logger, read_table, silver_dir

log = get_logger("model")
OPEN_END = "9999-12-31"
BEGINNING = "1900-01-01"

SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id  TEXT NOT NULL,
    full_name    TEXT,
    jmbg_hash    TEXT,
    city         TEXT,
    opened_date  TEXT,
    valid_from   TEXT NOT NULL,
    valid_to     TEXT NOT NULL,
    is_current   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dim_customer_id ON dim_customer(customer_id, is_current);

CREATE TABLE IF NOT EXISTS dim_account (
    account_id   TEXT PRIMARY KEY,
    customer_id  TEXT NOT NULL,
    account_type TEXT,
    currency     TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    date     TEXT NOT NULL,
    year     INTEGER, month INTEGER, day INTEGER, weekday INTEGER
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    txn_id       TEXT PRIMARY KEY,
    account_id   TEXT NOT NULL REFERENCES dim_account(account_id),
    customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    date_key     INTEGER NOT NULL REFERENCES dim_date(date_key),
    amount       REAL NOT NULL,
    txn_type     TEXT NOT NULL,
    status       TEXT NOT NULL,
    batch_date   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fact_date ON fact_transactions(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_customer ON fact_transactions(customer_key);
"""


def connect() -> sqlite3.Connection:
    db_path().parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path())
    conn.executescript(SCHEMA)
    return conn


def load_dim_account(conn, accounts: pd.DataFrame) -> None:
    conn.executemany(
        """INSERT INTO dim_account (account_id, customer_id, account_type, currency)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(account_id) DO UPDATE SET
             customer_id=excluded.customer_id, account_type=excluded.account_type,
             currency=excluded.currency""",
        accounts[["account_id", "customer_id", "account_type", "currency"]].itertuples(index=False))


def load_dim_date(conn, dates: pd.Series) -> None:
    rows = []
    for d in pd.to_datetime(pd.Series(dates).unique()):
        rows.append((int(d.strftime("%Y%m%d")), d.strftime("%Y-%m-%d"),
                     d.year, d.month, d.day, d.weekday()))
    conn.executemany("INSERT OR IGNORE INTO dim_date VALUES (?,?,?,?,?,?)", rows)


def load_dim_customer_scd2(conn, customers: pd.DataFrame, batch_date: str) -> dict:
    """SCD2 po atributu `city`. Idempotentno: ponovljen batch ne pravi nove verzije,
    a stariji batch (replay) ne moze da 'vrati' istoriju unazad."""
    current = {r[0]: r for r in conn.execute(
        "SELECT customer_id, customer_key, city, valid_from FROM dim_customer WHERE is_current=1")}
    inserted = changed = 0
    for r in customers.itertuples(index=False):
        row = current.get(r.customer_id)
        if row is None:
            conn.execute(
                """INSERT INTO dim_customer (customer_id, full_name, jmbg_hash, city, opened_date,
                                             valid_from, valid_to, is_current)
                   VALUES (?,?,?,?,?,?,?,1)""",
                (r.customer_id, r.full_name, r.jmbg_hash, r.city, r.opened_date, BEGINNING, OPEN_END))
            inserted += 1
            continue
        _, key, city, valid_from = row
        if batch_date <= valid_from or city == r.city:
            continue                                   # replay ili nema promene
        conn.execute("UPDATE dim_customer SET valid_to=?, is_current=0 WHERE customer_key=?",
                     (batch_date, key))
        conn.execute(
            """INSERT INTO dim_customer (customer_id, full_name, jmbg_hash, city, opened_date,
                                         valid_from, valid_to, is_current)
               VALUES (?,?,?,?,?,?,?,1)""",
            (r.customer_id, r.full_name, r.jmbg_hash, r.city, r.opened_date, batch_date, OPEN_END))
        changed += 1
    return {"new_customers": inserted, "scd2_changes": changed}


def load_fact(conn, txns: pd.DataFrame) -> int:
    txns.to_sql("stg_transactions", conn, if_exists="replace", index=False)
    before = conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
    conn.execute(
        """INSERT INTO fact_transactions
              (txn_id, account_id, customer_key, date_key, amount, txn_type, status, batch_date)
           SELECT s.txn_id, s.account_id, c.customer_key,
                  CAST(REPLACE(s.txn_date, '-', '') AS INTEGER),
                  s.amount, s.txn_type, s.status, s.batch_date
           FROM stg_transactions s
           JOIN dim_account  a ON a.account_id  = s.account_id
           JOIN dim_customer c ON c.customer_id = a.customer_id
                              AND s.txn_date >= c.valid_from AND s.txn_date < c.valid_to
           WHERE 1
           ON CONFLICT(txn_id) DO UPDATE SET
              amount=excluded.amount, status=excluded.status,
              customer_key=excluded.customer_key, batch_date=excluded.batch_date
           WHERE excluded.batch_date >= fact_transactions.batch_date""")  # stariji batch ne prepisuje noviji
    conn.execute("DROP TABLE stg_transactions")
    return conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0] - before


def model_batch(batch_date: str) -> dict:
    part = f"{PARTITION}{batch_date}"
    cust = read_table(silver_dir() / "customers" / part)
    acc = read_table(silver_dir() / "accounts" / part)
    txn = read_table(silver_dir() / "transactions" / part)

    conn = connect()
    try:
        with conn:                                    # jedna transakcija: ili sve ili nista
            stats = load_dim_customer_scd2(conn, cust, batch_date)
            load_dim_account(conn, acc)
            load_dim_date(conn, txn.txn_date)
            stats["new_facts"] = load_fact(conn, txn)
    finally:
        conn.close()
    log.info("gold %s: %s", batch_date, stats)
    return stats
