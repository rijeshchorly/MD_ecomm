SELECT "id",
    "user_id",
    COUNT(DISTINCT "id") AS "id_count_distinct",
    COALESCE(SUM("sale_price"), 0) AS "sale_price_sum"
FROM {{source('Ecomm', 'order_items')}} AS "main__order_items"
GROUP BY 1, 2
