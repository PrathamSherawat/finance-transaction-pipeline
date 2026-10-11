-- Daily KPIs per currency (UTC day of transaction_ts). POSTED only.
WITH agg AS (
    SELECT
        currency,
        CAST(transaction_ts AS DATE) AS txn_date,
        COALESCE(SUM(amount) FILTER (WHERE transaction_type = 'CREDIT'), 0) AS inflow,
        COALESCE(SUM(amount) FILTER (WHERE transaction_type = 'DEBIT'), 0) AS outflow,
        COUNT(*) AS txn_count
    FROM curated
    WHERE status = 'POSTED'
    GROUP BY currency, CAST(transaction_ts AS DATE)
)
SELECT currency, txn_date, inflow, outflow, inflow - outflow AS net, txn_count
FROM agg
ORDER BY currency, txn_date;
