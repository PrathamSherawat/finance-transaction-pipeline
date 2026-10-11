-- Totals per currency. Only POSTED rows count. Currencies are never added together.
WITH agg AS (
    SELECT
        currency,
        COALESCE(SUM(amount) FILTER (WHERE transaction_type = 'CREDIT'), 0) AS inflow,
        COALESCE(SUM(amount) FILTER (WHERE transaction_type = 'DEBIT'), 0) AS outflow,
        COUNT(*) AS txn_count
    FROM curated
    WHERE status = 'POSTED'
    GROUP BY currency
)
SELECT currency, inflow, outflow, inflow - outflow AS net, txn_count
FROM agg
ORDER BY currency;
