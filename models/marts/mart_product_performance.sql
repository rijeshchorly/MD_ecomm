/*
  mart_product_performance
  ------------------------
  Aggregates sales performance per product (units sold, revenue, margin,
  return rate). Useful for catalogue and merchandising decisions.
*/

with order_items as (
    select * from {{ ref('int_order_items_enriched') }}
    where status not in ('Cancelled')
),

product_agg as (
    select
        product_id,
        product_name,
        product_category,
        product_brand,
        product_department,

        count(order_item_id)                        as total_units_sold,
        count(distinct order_id)                    as total_orders,
        count(distinct user_id)                     as unique_buyers,

        round(sum(sale_price), 2)                   as total_revenue,
        round(sum(unit_cost), 2)                    as total_cost,
        round(sum(gross_profit), 2)                 as total_gross_profit,
        round(avg(sale_price), 2)                   as avg_sale_price,
        round(avg(gross_margin_pct), 4)             as avg_gross_margin_pct,

        sum(case when is_returned then 1 else 0 end)    as returned_units,
        round(
            sum(case when is_returned then 1 else 0 end)::float
            / nullif(count(order_item_id), 0),
            4
        )                                           as return_rate,

        round(avg(days_to_deliver), 1)              as avg_days_to_deliver,

        min(created_at)                             as first_sold_at,
        max(created_at)                             as last_sold_at

    from order_items
    group by 1, 2, 3, 4, 5
)

select * from product_agg
order by total_revenue desc
