WITH january_2021 AS (
    SELECT 
        ri.brandCode, 
        r.purchaseDate,
        COUNT(ri.receiptId) AS receipt_count
    FROM receiptItem ri
-- Join with receipts and receiptItem table using receiptId
    JOIN receipts r ON ri.receiptId = r.receiptId  
    WHERE r.purchaseDate >= '2021-01-01' 
      AND r.purchaseDate < '2021-02-01'
    GROUP BY ri.brandCode, r.purchaseDate
)
SELECT 
    b.brandId,j.brandCode, b.brandName,j.purchaseDate, j.receipt_count
   FROM january_2021 j
  JOIN brand b ON j.brandCode = b.brandCode
ORDER BY j.receipt_count DESC
LIMIT 5;
