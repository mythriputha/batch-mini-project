-- Databricks SQL validation / reconciliation queries
-- Replace <catalog>.<schema> before running (or run in a notebook that has already
-- executed `USE CATALOG <catalog>; USE SCHEMA <schema>;`)

-- 1. Row counts per layer
SELECT 'bronze_trades' AS table_name, COUNT(*) AS row_count FROM <catalog>.<schema>.bronze_trades
UNION ALL
SELECT 'silver_trades', COUNT(*) FROM <catalog>.<schema>.silver_trades
UNION ALL
SELECT 'quarantine_trades', COUNT(*) FROM <catalog>.<schema>.quarantine_trades
UNION ALL
SELECT 'gold_daily_position', COUNT(*) FROM <catalog>.<schema>.gold_daily_position;

-- 2. Row-count reconciliation: bronze = silver + quarantine
SELECT
  (SELECT COUNT(*) FROM <catalog>.<schema>.bronze_trades) AS bronze_rows,
  (SELECT COUNT(*) FROM <catalog>.<schema>.silver_trades) AS silver_rows,
  (SELECT COUNT(*) FROM <catalog>.<schema>.quarantine_trades) AS quarantine_rows,
  (SELECT COUNT(*) FROM <catalog>.<schema>.silver_trades) + (SELECT COUNT(*) FROM <catalog>.<schema>.quarantine_trades) AS silver_plus_quarantine,
  (SELECT COUNT(*) FROM <catalog>.<schema>.bronze_trades)
    = (SELECT COUNT(*) FROM <catalog>.<schema>.silver_trades) + (SELECT COUNT(*) FROM <catalog>.<schema>.quarantine_trades) AS reconciled;

-- 3. Quarantine reasons breakdown
SELECT _dq_reason, COUNT(*) AS rows
FROM <catalog>.<schema>.quarantine_trades
GROUP BY _dq_reason
ORDER BY rows DESC;

-- 4. Quarantine rate (alert if > 2%)
SELECT
  100.0 * (SELECT COUNT(*) FROM <catalog>.<schema>.quarantine_trades)
        / (SELECT COUNT(*) FROM <catalog>.<schema>.bronze_trades) AS quarantine_pct;

-- 5. Orphan account keys (should be 0 after conformed join, or investigate)
SELECT trade_id, account_id
FROM <catalog>.<schema>.silver_trades
WHERE account_type IS NULL;

-- 6. Duplicate trade_id check on silver (should return 0 rows)
SELECT trade_id, COUNT(*) AS cnt
FROM <catalog>.<schema>.silver_trades
GROUP BY trade_id
HAVING COUNT(*) > 1;

-- 7. Financial reconciliation: silver trade_value sum vs gold gross_value sum
SELECT
  (SELECT SUM(trade_value) FROM <catalog>.<schema>.silver_trades) AS silver_gross_value,
  (SELECT SUM(gross_value) FROM <catalog>.<schema>.gold_daily_position) AS gold_gross_value;

-- 8. Gold business view
SELECT * FROM <catalog>.<schema>.gold_daily_position ORDER BY trade_date, symbol;

-- 9. Delta history / time travel exercises
DESCRIBE HISTORY <catalog>.<schema>.silver_trades;

-- SELECT * FROM <catalog>.<schema>.silver_trades VERSION AS OF 0;
-- SELECT * FROM <catalog>.<schema>.silver_trades TIMESTAMP AS OF '2026-09-03T00:00:00Z';
