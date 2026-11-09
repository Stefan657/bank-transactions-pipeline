"""Orkestracija: generate -> ingest (bronze) -> clean (silver) -> model (gold).

Koriscenje:
    python -m src.run_pipeline --generate          # generise podatke i pokrece sve batch-eve
    python -m src.run_pipeline --batch 2026-11-02  # samo jedan batch
U produkciji ovu ulogu preuzima Airflow / Azure Data Factory / Databricks Workflows.
"""
import argparse
import time

from .clean import clean_batch
from .common import get_logger, source_dir
from .generate_data import generate_all
from .ingest import ingest_batch
from .model import model_batch

log = get_logger("pipeline")


def run_batch(batch_date: str, retries: int = 2) -> dict:
    for attempt in range(1, retries + 2):
        try:
            ingest_batch(batch_date)
            silver = clean_batch(batch_date)
            gold = model_batch(batch_date)
            return {**silver, **gold}
        except Exception:
            log.exception("batch %s pao (pokusaj %d)", batch_date, attempt)
            if attempt > retries:
                raise
            time.sleep(1)  # u produkciji: exponential backoff


def run_all() -> None:
    for d in sorted(p.name for p in source_dir().iterdir() if p.is_dir()):
        run_batch(d)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--generate", action="store_true", help="prvo generisi sintetičke podatke")
    ap.add_argument("--batch", help="obradi samo jedan batch (YYYY-MM-DD)")
    args = ap.parse_args()
    if args.generate:
        generate_all()
    run_batch(args.batch) if args.batch else run_all()
