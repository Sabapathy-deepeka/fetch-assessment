WITH brand_ranking AS (
    SELECT 
        ri.brandCode, 
        -- Extracting the month
        DATE_TRUNC('month', r.purchaseDate) AS month,  
        COUNT(DISTINCT(ri.receiptId)) AS receipt_count  
    FROM receiptItem ri
    JOIN receipts r ON ri.receiptId = r.receiptId
    WHERE r.purchaseDate >= '2021-01-01' 
      AND r.purchaseDate < '2021-02-01'  
    GROUP BY ri.brandCode, month
),
ranked_brands AS (
    SELECT br.brandCode, br.month, br.receipt_count,
        RANK() OVER (PARTITION BY month ORDER BY br.receipt_count DESC) AS rank
    FROM brand_ranking br
)
SELECT  DISTINCT(b.brandId), rb.brandCode, rb.month, rb.receipt_count, rb.rank
FROM ranked_brands rb 
JOIN brand b ON rb.brandCode = b.brandCode
ORDER BY rb.month DESC, rb.rank ASC
LIMIT 5;
