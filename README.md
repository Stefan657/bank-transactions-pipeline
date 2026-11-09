# Bank Transactions Mini Pipeline

End-to-end data engineering projekat na **sintetičkim** bankarskim podacima (ništa stvarno).
Pokazuje: SQL, Python, medallion arhitekturu (bronze/silver/gold), star schema, SCD tip 2,
inkrementalno učitavanje, idempotentnost, kvalitet podataka, zaštitu ličnih podataka i testove.

## Arhitektura

```
 source CSV (core banking simulacija)
        │  batch po danu
        ▼
 BRONZE  – sirovi fajlovi 1:1, particija ingest_date=...        (src/ingest.py)
        ▼
 SILVER  – čišćenje, tipovi, dedup, validacija,                  (src/clean.py)
           pseudonimizacija JMBG, rejects sa razlogom
        ▼
 GOLD    – star schema: fact_transactions, dim_customer (SCD2),  (src/model.py)
           dim_account, dim_date
        ▼
 SQL analitika (window funkcije, anomalije, istorija)            (sql/analytics.sql)
```

## Pokretanje

```bash
pip install -r requirements.txt
python -m src.run_pipeline --generate   # generiše podatke i pokreće ceo pipeline
python -m src.run_analytics             # 6 analitičkih upita
pytest -q                               # testovi kvaliteta i idempotentnosti
```

## Ključne dizajn odluke

| Tema | Odluka | Zašto |
|---|---|---|
| Slojevi | bronze → silver → gold | sirovi podaci ostaju netaknuti, može se ponovo obraditi |
| Učitavanje | inkrementalno po `ingest_date` particiji | ne obrađuje se sve iznova |
| Idempotentnost | prepis particije + `UPSERT` po `txn_id` | bezbedno ponovo pokretanje, bez duplikata |
| Istorija | SCD2 na `city` (`valid_from`/`valid_to`/`is_current`) | izveštaj tačan "as-of" datuma transakcije |
| Loši podaci | odbacuju se u `rejects` sa razlogom i originalnim zapisom | auditabilnost, ništa se ne gubi tiho |
| Lični podaci | JMBG se heširaju (SHA-256 + salt), sirovi ne ide dalje od bronze | pseudonimizacija, GDPR/ZZPL |
| Atomičnost | gold punjenje u jednoj DB transakciji | ili sve ili ništa |
| Replay zaštita | stariji batch ne prepisuje noviji podatak (`WHERE excluded.batch_date >= batch_date`) i ne kvari SCD2 istoriju | kasno stigli i ponovo poslati podaci |

## Kako ovo mapira na cloud

| Lokalno | Azure | AWS | GCP |
|---|---|---|---|
| `data/lake` folderi | ADLS Gen2 | S3 | Cloud Storage |
| pandas skripte | Databricks / Synapse Spark | Glue / EMR | Dataproc |
| SQLite gold | Synapse / Fabric Warehouse | Redshift | BigQuery |
| `run_pipeline.py` | Data Factory / Workflows | Step Functions / MWAA | Composer (Airflow) |
| `PII_SALT` env var | Key Vault | Secrets Manager | Secret Manager |
| GitHub Actions | GitHub Actions / Azure DevOps | isto | isto |

## Ograničenja (iskreno)

- Radi lokalno; cloud verzija zahteva zamenu I/O sloja i infrastrukture (vidi tabelu).
- SQLite nije distribuirano skladište; za veće količine ide Spark + cloud warehouse.
- Nema streaming dela ni Terraform-a (ideje za sledeći korak).

## Ideje za nadogradnju

1. Deploy na jedan cloud (free tier) + Terraform za storage i bazu
2. Airflow DAG umesto `run_pipeline.py`
3. dbt umesto ručnih SQL transformacija
4. Great Expectations ili dbt testovi za kvalitet
5. Mali streaming demo (Kafka / Event Hubs)
