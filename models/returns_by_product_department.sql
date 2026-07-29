SELECT "returned_units" * 2 AS "returned_units_count",
    "returned_units",
    "product_department"
FROM {{ref('mart_category_performance')}} AS "omni_dbt_marts__mart_category_performance"
WHERE "product_department" = 'Women'
GROUP BY 3, 2, 1
