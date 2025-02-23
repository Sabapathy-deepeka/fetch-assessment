SELECT 
    rewardsReceiptStatus, 
    AVG(cast(totalSpent AS NUMERIC)) AS avg_spend
FROM receipts
WHERE rewardsReceiptStatus IN ('FINISHED', 'REJECTED')
GROUP BY rewardsReceiptStatus;
