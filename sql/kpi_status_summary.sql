-- Sanity check: row counts per status over ALL curated rows (not a financial KPI).
SELECT status, COUNT(*) AS txn_count
FROM curated
GROUP BY status
ORDER BY status;
