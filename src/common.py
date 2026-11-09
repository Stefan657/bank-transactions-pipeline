"""Zajednicke putanje i I/O pomocne funkcije.

Lokalni folder `data/lake` simulira cloud storage (data lake).
U cloud-u se samo menja BASE putanja i I/O sloj:
  Azure: abfss://lake@<account>.dfs.core.windows.net/
  AWS:   s3://<bucket>/
  GCP:   gs://<bucket>/
"""
import logging
import os
from pathlib import Path

import pandas as pd

PARTITION = "ingest_date="


def base() -> Path:
    default = Path(__file__).resolve().parent.parent / "data"
    return Path(os.environ.get("BANK_DATA_DIR", default))


def source_dir() -> Path:
    return base() / "source"


def bronze_dir() -> Path:
    return base() / "lake" / "bronze"


def silver_dir() -> Path:
    return base() / "lake" / "silver"


def db_path() -> Path:
    return base() / "warehouse.db"


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return logging.getLogger(name)


def write_table(df: pd.DataFrame, directory: Path) -> Path:
    """Upisuje tabelu kao Parquet ako je pyarrow dostupan, inace CSV."""
    directory.mkdir(parents=True, exist_ok=True)
    for old in directory.glob("data.*"):
        old.unlink()  # overwrite iste particije => idempotentno
    try:
        import pyarrow  # noqa: F401
        path = directory / "data.parquet"
        df.to_parquet(path, index=False)
    except ImportError:
        path = directory / "data.csv"
        df.to_csv(path, index=False, encoding="utf-8")
    return path


def read_table(directory: Path) -> pd.DataFrame:
    parquet, csv = directory / "data.parquet", directory / "data.csv"
    if parquet.exists():
        return pd.read_parquet(parquet)
    return pd.read_csv(csv, encoding="utf-8")


def read_partitions(root: Path, upto: str | None = None) -> pd.DataFrame:
    """Cita sve particije ingest_date=YYYY-MM-DD (opciono do datuma upto)."""
    frames = []
    for part in sorted(root.glob(f"{PARTITION}*")):
        d = part.name.removeprefix(PARTITION)
        if upto is None or d <= upto:
            frames.append(read_table(part))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
