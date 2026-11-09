"""Testovi kvaliteta podataka i idempotentnosti. Pokretanje: pytest -q"""
import os
import sqlite3
import tempfile

from src import run_pipeline
from src.common import db_path
from src.generate_data import generate_all

_CACHE = {}


def _setup():
    """Jednom pokrece ceo pipeline u privremenom folderu."""
    if "dir" not in _CACHE:
        tmp = tempfile.mkdtemp(prefix="bank_")
        os.environ["BANK_DATA_DIR"] = tmp
        generate_all()
        run_pipeline.run_all()
        _CACHE["dir"] = tmp
    os.environ["BANK_DATA_DIR"] = _CACHE["dir"]
    return sqlite3.connect(db_path())


def _scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


def test_no_duplicate_transactions():
    conn = _setup()
    assert _scalar(conn, "SELECT COUNT(*) - COUNT(DISTINCT txn_id) FROM fact_transactions") == 0


def test_amounts_valid():
    conn = _setup()
    assert _scalar(conn, "SELECT COUNT(*) FROM fact_transactions WHERE amount IS NULL") == 0
    assert _scalar(conn, "SELECT COUNT(*) FROM fact_transactions "
                         "WHERE amount < 0 AND txn_type <> 'REFUND'") == 0


def test_referential_integrity():
    conn = _setup()
    assert _scalar(conn, """SELECT COUNT(*) FROM fact_transactions f
                            LEFT JOIN dim_account a ON a.account_id = f.account_id
                            WHERE a.account_id IS NULL""") == 0
    assert _scalar(conn, """SELECT COUNT(*) FROM fact_transactions f
                            LEFT JOIN dim_customer c ON c.customer_key = f.customer_key
                            WHERE c.customer_key IS NULL""") == 0


def test_scd2_exactly_one_current_row_per_customer():
    conn = _setup()
    bad = _scalar(conn, """SELECT COUNT(*) FROM (
                             SELECT customer_id FROM dim_customer
                             GROUP BY customer_id HAVING SUM(is_current) <> 1)""")
    assert bad == 0


def test_scd2_no_overlapping_versions():
    conn = _setup()
    overlap = _scalar(conn, """SELECT COUNT(*) FROM dim_customer a JOIN dim_customer b
                               ON a.customer_id = b.customer_id
                              AND a.customer_key < b.customer_key
                              AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to""")
    assert overlap == 0


def test_scd2_captured_the_10_city_changes():
    conn = _setup()
    assert _scalar(conn, "SELECT COUNT(*) FROM dim_customer WHERE is_current = 0") == 10


def test_replay_updates_existing_rows_without_duplicating():
    """Transakcije iz dana 1 ponovo poslate u danu 2 azuriraju postojece redove (upsert)."""
    conn = _setup()
    replayed = _scalar(conn, """SELECT COUNT(*) FROM fact_transactions
                                WHERE batch_date = '2026-11-02' AND txn_id <= 'T0004000'""")
    assert replayed > 0
    assert _scalar(conn, "SELECT COUNT(*) - COUNT(DISTINCT txn_id) FROM fact_transactions") == 0


def test_pipeline_is_idempotent():
    conn = _setup()
    before = (_scalar(conn, "SELECT COUNT(*) FROM fact_transactions"),
              _scalar(conn, "SELECT COUNT(*) FROM dim_customer"),
              _scalar(conn, "SELECT ROUND(SUM(amount),2) FROM fact_transactions"))
    conn.close()
    run_pipeline.run_all()                      # ponovo pusti SVE
    run_pipeline.run_batch("2026-11-01")        # i stariji batch nakon novijeg (replay)
    conn = sqlite3.connect(db_path())
    after = (_scalar(conn, "SELECT COUNT(*) FROM fact_transactions"),
             _scalar(conn, "SELECT COUNT(*) FROM dim_customer"),
             _scalar(conn, "SELECT ROUND(SUM(amount),2) FROM fact_transactions"))
    assert before == after


def test_raw_jmbg_never_reaches_gold():
    conn = _setup()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(dim_customer)")]
    assert "jmbg" not in cols and "jmbg_hash" in cols


def test_old_batch_replay_does_not_overwrite_newer_data():
    """Ponovno pustanje starijeg batch-a ne sme da vrati stari status/batch_date."""
    conn = _setup()
    run_pipeline.run_batch("2026-11-01")        # stariji batch posle novijeg
    conn = sqlite3.connect(db_path())
    replayed = _scalar(conn, """SELECT COUNT(*) FROM fact_transactions
                                WHERE batch_date = '2026-11-02' AND txn_id <= 'T0004000'""")
    assert replayed > 0
