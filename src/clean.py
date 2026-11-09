"""SILVER sloj: ciscenje, tipovi, deduplikacija, validacija i maskiranje.

Nevalidni redovi se NE brisu tiho: upisuju se u rejects sa razlogom i originalnim
zapisom (auditabilnost - u banci je to obavezno).
"""
import hashlib
import json
import os

import pandas as pd

from .common import (PARTITION, bronze_dir, get_logger, read_partitions,
                     silver_dir, write_table)

log = get_logger("clean")


def _raw_json(df: pd.DataFrame) -> pd.Series:
    return df.apply(lambda r: json.dumps(r.to_dict(), ensure_ascii=False), axis=1)


def _mark(reason: pd.Series, mask: pd.Series, text: str) -> None:
    reason[mask & reason.isna()] = text


def _split(df: pd.DataFrame, reason: pd.Series, table: str, cols: list[str]):
    rejects = pd.DataFrame({"table": table, "reason": reason[reason.notna()],
                            "raw_record": df.loc[reason.notna(), "_raw"]})
    return df.loc[reason.isna(), cols].reset_index(drop=True), rejects


def _hash_pii(value: str) -> str:
    """Pseudonimizacija: SHA-256 sa tajnim 'salt'-om. U produkciji salt dolazi iz Key Vault-a."""
    salt = os.environ.get("PII_SALT", "dev-only-salt")
    return hashlib.sha256((salt + str(value)).encode()).hexdigest()[:16]


def clean_customers(raw: pd.DataFrame, batch_date: str):
    df = raw.astype(str).copy()
    df["_raw"] = _raw_json(raw.astype(str))
    for c in ["customer_id", "full_name", "jmbg", "city", "opened_date"]:
        df[c] = df[c].str.strip().replace("nan", "")
    reason = pd.Series([None] * len(df), index=df.index, dtype=object)
    _mark(reason, df.customer_id == "", "missing_customer_id")
    _mark(reason, ~df.jmbg.str.fullmatch(r"\d{13}"), "invalid_jmbg")
    _mark(reason, df.duplicated("customer_id", keep="first"), "duplicate")
    df["jmbg_hash"] = df.jmbg.map(_hash_pii)       # sirovi JMBG ne ide dalje od bronze
    df["batch_date"] = batch_date
    return _split(df, reason, "customers",
                  ["customer_id", "full_name", "jmbg_hash", "city", "opened_date", "batch_date"])


def clean_accounts(raw: pd.DataFrame, batch_date: str):
    df = raw.astype(str).copy()
    df["_raw"] = _raw_json(raw.astype(str))
    for c in ["account_id", "customer_id", "account_type", "currency"]:
        df[c] = df[c].str.strip().replace("nan", "")
    df["account_type"] = df.account_type.str.upper()
    df["currency"] = df.currency.str.upper()
    reason = pd.Series([None] * len(df), index=df.index, dtype=object)
    _mark(reason, df.account_id == "", "missing_account_id")
    _mark(reason, df.duplicated("account_id", keep="first"), "duplicate")
    df["batch_date"] = batch_date
    return _split(df, reason, "accounts",
                  ["account_id", "customer_id", "account_type", "currency", "batch_date"])


def clean_transactions(raw: pd.DataFrame, known_accounts: set, batch_date: str):
    df = raw.astype(str).copy()
    df["_raw"] = _raw_json(raw.astype(str))
    for c in ["txn_id", "account_id", "txn_type", "status"]:
        df[c] = df[c].str.strip().replace("nan", "")
    df["txn_type"] = df.txn_type.str.upper()
    df["status"] = df.status.str.upper()
    df["amount"] = pd.to_numeric(df.amount, errors="coerce")
    d_iso = pd.to_datetime(df.txn_date, format="%Y-%m-%d", errors="coerce")
    d_eu = pd.to_datetime(df.txn_date, format="%d.%m.%Y", errors="coerce")
    df["txn_date"] = d_iso.fillna(d_eu)

    reason = pd.Series([None] * len(df), index=df.index, dtype=object)
    _mark(reason, df.txn_id == "", "missing_txn_id")
    _mark(reason, df.amount.isna(), "null_or_invalid_amount")
    _mark(reason, df.txn_date.isna(), "bad_date")
    _mark(reason, (df.amount < 0) & (df.txn_type != "REFUND"), "negative_amount")
    _mark(reason, ~df.account_id.isin(known_accounts), "orphan_account")
    valid = reason.isna()
    _mark(reason, valid & df.duplicated("txn_id", keep="first"), "duplicate")  # ceo red je dup

    df["txn_date"] = df.txn_date.dt.strftime("%Y-%m-%d")
    df["batch_date"] = batch_date
    return _split(df, reason, "transactions",
                  ["txn_id", "account_id", "amount", "txn_date", "txn_type", "status", "batch_date"])


def clean_batch(batch_date: str) -> dict:
    def raw(table):
        return pd.read_csv(bronze_dir() / table / f"{PARTITION}{batch_date}" / f"{table}.csv",
                           dtype=str, keep_default_na=False, encoding="utf-8")

    stats, all_rejects = {}, []
    cust, rej = clean_customers(raw("customers"), batch_date); all_rejects.append(rej)
    write_table(cust, silver_dir() / "customers" / f"{PARTITION}{batch_date}")
    acc, rej = clean_accounts(raw("accounts"), batch_date); all_rejects.append(rej)
    write_table(acc, silver_dir() / "accounts" / f"{PARTITION}{batch_date}")

    known = set(read_partitions(silver_dir() / "accounts", upto=batch_date).account_id)
    txn_raw = raw("transactions")
    txn, rej = clean_transactions(txn_raw, known, batch_date); all_rejects.append(rej)
    write_table(txn, silver_dir() / "transactions" / f"{PARTITION}{batch_date}")

    rejects = pd.concat(all_rejects, ignore_index=True)
    write_table(rejects, silver_dir() / "rejects" / f"{PARTITION}{batch_date}")

    stats = {"customers": len(cust), "accounts": len(acc),
             "transactions_in": len(txn_raw), "transactions_ok": len(txn),
             "rejected": len(rejects)}
    log.info("silver %s: %s", batch_date, stats)
    log.info("razlozi odbacivanja: %s", rejects.reason.value_counts().to_dict())
    return stats
