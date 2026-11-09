"""Generise SINTETICKE bankarske podatke sa namerno 'prljavim' redovima.

Dva batch-a (kao dva dana ekstrakcije iz core banking sistema):
  2026-11-01: pocetno punjenje (transakcije oktobra)
  2026-11-02: 10 komitenata menja grad (SCD2), 5 novih komitenata, nove transakcije
              + ponovo poslate (replay) transakcije iz prethodnog dana (test idempotentnosti)
"""
import random
from datetime import date, timedelta

import pandas as pd

from .common import get_logger, source_dir

log = get_logger("generate")

FIRST = ["Marko", "Jovana", "Nikola", "Milica", "Stefan", "Ana", "Luka", "Jelena", "Petar", "Ivana"]
LAST = ["Jovanovic", "Petrovic", "Nikolic", "Markovic", "Djordjevic", "Stojanovic", "Ilic", "Pavlovic"]
CITIES = ["Beograd", "Novi Sad", "Nis", "Kragujevac", "Subotica", "Smederevo", "Cacak", "Kraljevo"]
TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT", "REFUND"]
STATUSES = ["COMPLETED"] * 8 + ["PENDING", "FAILED"]


def _customers(rng, start_id, n):
    rows = []
    for i in range(start_id, start_id + n):
        rows.append({
            "customer_id": f"C{i:05d}",
            "full_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",
            "jmbg": "".join(rng.choice("0123456789") for _ in range(13)),  # IZMISLJEN
            "city": rng.choice(CITIES),
            "opened_date": (date(2015, 1, 1) + timedelta(days=rng.randint(0, 3600))).isoformat(),
        })
    return rows


def _accounts(rng, customers, start_id):
    rows, i = [], start_id
    for c in customers:
        for _ in range(rng.choice([1, 1, 2])):
            rows.append({"account_id": f"A{i:06d}", "customer_id": c["customer_id"],
                         "account_type": rng.choice(["CURRENT", "SAVINGS"]),
                         "currency": rng.choice(["RSD", "RSD", "EUR"])})
            i += 1
    return rows


def _transactions(rng, accounts, start_id, n, d_from, d_to):
    days = (d_to - d_from).days
    rows = []
    for i in range(start_id, start_id + n):
        rows.append({
            "txn_id": f"T{i:07d}",
            "account_id": rng.choice(accounts)["account_id"],
            "amount": round(rng.lognormvariate(5, 1.3), 2),
            "txn_date": (d_from + timedelta(days=rng.randint(0, days))).isoformat(),
            "txn_type": rng.choice(TYPES),
            "status": rng.choice(STATUSES),
        })
    return rows


def _make_dirty(rng, txns):
    """Ubacuje tipicne probleme kvaliteta podataka."""
    out = [dict(t) for t in txns]
    for t in out:
        r = rng.random()
        if r < 0.01:
            t["amount"] = ""                                  # nedostaje iznos
        elif r < 0.02 and t["txn_type"] != "REFUND":
            t["amount"] = -abs(float(t["amount"]))            # negativan iznos
        elif r < 0.05:
            y, m, d = t["txn_date"].split("-")
            t["txn_date"] = f"{d}.{m}.{y}"                    # drugi format datuma
        elif r < 0.06:
            t["account_id"] = "A999999"                       # nepostojeci racun
        elif r < 0.08:
            t["txn_type"] = f" {t['txn_type'].lower()} "      # razmaci + mala slova
    out += [dict(t) for t in rng.sample(out, k=max(1, len(out) // 50))]  # duplikati
    rng.shuffle(out)
    return out


def _write(batch_date, customers, accounts, txns):
    d = source_dir() / batch_date
    d.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(customers).to_csv(d / "customers.csv", index=False, encoding="utf-8")
    pd.DataFrame(accounts).to_csv(d / "accounts.csv", index=False, encoding="utf-8")
    pd.DataFrame(txns).to_csv(d / "transactions.csv", index=False, encoding="utf-8")
    log.info("batch %s: %d komitenata, %d racuna, %d transakcija",
             batch_date, len(customers), len(accounts), len(txns))


def generate_all(seed: int = 42):
    rng = random.Random(seed)

    # --- Dan 1: pocetno punjenje ---
    customers = _customers(rng, 1, 200)
    accounts = _accounts(rng, customers, 1)
    clean1 = _transactions(rng, accounts, 1, 4000, date(2026, 10, 1), date(2026, 10, 31))
    _write("2026-11-01", customers, accounts, _make_dirty(rng, clean1))

    # --- Dan 2: promene + novi podaci ---
    changed = rng.sample(range(len(customers)), 10)
    customers = [dict(c) for c in customers]
    for idx in changed:
        old = customers[idx]["city"]
        customers[idx]["city"] = rng.choice([c for c in CITIES if c != old])
    new_customers = _customers(rng, 201, 5)
    new_accounts = _accounts(rng, new_customers, len(accounts) + 1)
    customers += new_customers
    accounts += new_accounts
    new_txns = _transactions(rng, accounts, 4001, 1000, date(2026, 11, 1), date(2026, 11, 2))
    replay = []
    for t in rng.sample(clean1, 30):                           # kasno stigle / ponovo poslate
        t = dict(t)
        if t["status"] == "PENDING":
            t["status"] = "COMPLETED"                          # status se promenio
        replay.append(t)
    _write("2026-11-02", customers, accounts, _make_dirty(rng, new_txns + replay))


if __name__ == "__main__":
    generate_all()
