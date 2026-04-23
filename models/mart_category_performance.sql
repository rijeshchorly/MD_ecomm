SELECT "product_department",
    "product_category",
    "unique_buyers",
    "returned_units",
    "returned_units" * 2 AS "returned_units_double"
FROM {{ref('mart_category_performance')}} AS "omni_dbt_marts__mart_category_performance"
GROUP BY 2, 1, 4, 5, 3
