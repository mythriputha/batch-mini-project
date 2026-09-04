-- Databricks SQL validation / reconciliation queries — resolved for the scheduled job.
-- This is a fixed-catalog copy of validation.sql (which stays generic/templated for manual use).
-- If the catalog/schema ever changes, update both this file and resources/job.yml's base_parameters.

-- 1. Row counts per layer
SELECT 'bronze_trades' AS table_name, COUNT(*) AS row_count FROM dbx_batch_mini_ws.batch_mini_project.bronze_trades
UNION ALL
SELECT 'silver_trades', COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.silver_trades
UNION ALL
SELECT 'quarantine_trades', COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.quarantine_trades
UNION ALL
SELECT 'gold_daily_position', COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.gold_daily_position;

-- 2. Row-count reconciliation: bronze = silver + quarantine
SELECT
  (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.bronze_trades) AS bronze_rows,
  (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.silver_trades) AS silver_rows,
  (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.quarantine_trades) AS quarantine_rows,
  (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.silver_trades) + (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.quarantine_trades) AS silver_plus_quarantine,
  (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.bronze_trades)
    = (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.silver_trades) + (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.quarantine_trades) AS reconciled;

-- 3. Quarantine reasons breakdown
SELECT _dq_reason, COUNT(*) AS rows
FROM dbx_batch_mini_ws.batch_mini_project.quarantine_trades
GROUP BY _dq_reason
ORDER BY rows DESC;

-- 4. Quarantine rate (alert if > 2%)
SELECT
  100.0 * (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.quarantine_trades)
        / (SELECT COUNT(*) FROM dbx_batch_mini_ws.batch_mini_project.bronze_trades) AS quarantine_pct;

-- 5. Orphan account keys (should be 0 after conformed join, or investigate)
SELECT trade_id, account_id
FROM dbx_batch_mini_ws.batch_mini_project.silver_trades
WHERE account_type IS NULL;

-- 6. Duplicate trade_id check on silver (should return 0 rows)
SELECT trade_id, COUNT(*) AS cnt
FROM dbx_batch_mini_ws.batch_mini_project.silver_trades
GROUP BY trade_id
HAVING COUNT(*) > 1;

-- 7. Financial reconciliation: silver trade_value sum vs gold gross_value sum
SELECT
  (SELECT SUM(trade_value) FROM dbx_batch_mini_ws.batch_mini_project.silver_trades) AS silver_gross_value,
  (SELECT SUM(gross_value) FROM dbx_batch_mini_ws.batch_mini_project.gold_daily_position) AS gold_gross_value;

-- 8. Gold business view
SELECT * FROM dbx_batch_mini_ws.batch_mini_project.gold_daily_position ORDER BY trade_date, symbol;
