"""BRONZE sloj: sirovi fajlovi se kopiraju 1:1, bez ikakve izmene.

Particionisanje po datumu ucitavanja (ingest_date=...). Ponovno pokretanje
za isti datum prepisuje istu particiju => idempotentno.
"""
import shutil

from .common import PARTITION, bronze_dir, get_logger, source_dir

log = get_logger("ingest")
TABLES = ["customers", "accounts", "transactions"]


def ingest_batch(batch_date: str) -> None:
    src = source_dir() / batch_date
    if not src.exists():
        raise FileNotFoundError(f"Nema izvornih fajlova za {batch_date}: {src}")
    for table in TABLES:
        dest = bronze_dir() / table / f"{PARTITION}{batch_date}"
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src / f"{table}.csv", dest / f"{table}.csv")
        log.info("bronze: %s/%s kopiran", table, batch_date)
