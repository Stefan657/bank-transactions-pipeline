-- name: 1_monthly_turnover_by_customer
-- Mesecni promet po komitentu (samo uspesne transakcije)
SELECT c.customer_id, d.year, d.month,
       COUNT(*)            AS txn_count,
       ROUND(SUM(f.amount), 2) AS total_amount
FROM fact_transactions f
JOIN dim_customer c ON c.customer_key = f.customer_key
JOIN dim_date d     ON d.date_key = f.date_key
WHERE f.status = 'COMPLETED'
GROUP BY c.customer_id, d.year, d.month
ORDER BY total_amount DESC
LIMIT 10;

-- name: 2_top10_customers
-- Top 10 komitenata po ukupnom prometu (agregacija po prirodnom kljucu, ne po verziji reda)
SELECT c.customer_id, ROUND(SUM(f.amount), 2) AS total_amount
FROM fact_transactions f
JOIN dim_customer c ON c.customer_key = f.customer_key
WHERE f.status = 'COMPLETED'
GROUP BY c.customer_id
ORDER BY total_amount DESC
LIMIT 10;

-- name: 3_last_transaction_per_account
-- Window funkcija: poslednja transakcija po racunu
SELECT account_id, txn_id, txn_date, amount FROM (
    SELECT f.account_id, f.txn_id, d.date AS txn_date, f.amount,
           ROW_NUMBER() OVER (PARTITION BY f.account_id
                              ORDER BY d.date DESC, f.txn_id DESC) AS rn
    FROM fact_transactions f JOIN dim_date d ON d.date_key = f.date_key
) WHERE rn = 1
LIMIT 10;

-- name: 4_running_total_by_month
-- Window funkcija: kumulativni promet po mesecima za jedan racun
WITH monthly AS (
    SELECT f.account_id, d.year, d.month, SUM(f.amount) AS month_amount
    FROM fact_transactions f JOIN dim_date d ON d.date_key = f.date_key
    WHERE f.status = 'COMPLETED'
    GROUP BY f.account_id, d.year, d.month
)
SELECT account_id, year, month, ROUND(month_amount, 2) AS month_amount,
       ROUND(SUM(month_amount) OVER (PARTITION BY account_id
                                     ORDER BY year, month), 2) AS running_total
FROM monthly
ORDER BY account_id, year, month
LIMIT 10;

-- name: 5_anomalies_3x_account_average
-- Jednostavna detekcija anomalija: transakcije vece od 3x proseka racuna
WITH x AS (
    SELECT f.txn_id, f.account_id, f.amount,
           AVG(f.amount) OVER (PARTITION BY f.account_id) AS avg_amount
    FROM fact_transactions f WHERE f.status = 'COMPLETED'
)
SELECT txn_id, account_id, amount, ROUND(avg_amount, 2) AS avg_amount,
       ROUND(amount / avg_amount, 1) AS times_avg
FROM x WHERE amount > 3 * avg_amount
ORDER BY times_avg DESC
LIMIT 10;

-- name: 6_scd2_history
-- Istorija promene grada (SCD tip 2)
SELECT customer_id, city, valid_from, valid_to, is_current
FROM dim_customer
WHERE customer_id IN (SELECT customer_id FROM dim_customer GROUP BY customer_id HAVING COUNT(*) > 1)
ORDER BY customer_id, valid_from
LIMIT 10;
