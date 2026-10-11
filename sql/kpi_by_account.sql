-- KPIs per currency and account. POSTED only.
WITH agg AS (
    SELECT
        currency,
        account_id,
        COALESCE(SUM(amount) FILTER (WHERE transaction_type = 'CREDIT'), 0) AS inflow,
        COALESCE(SUM(amount) FILTER (WHERE transaction_type = 'DEBIT'), 0) AS outflow,
        COUNT(*) AS txn_count
    FROM curated
    WHERE status = 'POSTED'
    GROUP BY currency, account_id
)
SELECT currency, account_id, inflow, outflow, inflow - outflow AS net, txn_count
FROM agg
ORDER BY currency, account_id;
