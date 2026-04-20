/*
  mart_category_performance
  -------------------------
  Monthly revenue and margin breakdown by product category.
  Useful for identifying seasonal trends and category health.
*/

with order_items as (
    select * from {{ ref('int_order_items_enriched') }}
    where status not in ('Cancelled')
),

monthly_category as (
    select
        date_trunc('month', created_at)::date   as month,
        product_category,
        product_department,

        count(order_item_id)                    as units_sold,
        count(distinct order_id)                as orders,
        count(distinct user_id)                 as unique_buyers,

        round(sum(sale_price), 2)               as revenue,
        round(sum(unit_cost), 2)                as cost,
        round(sum(gross_profit), 2)             as gross_profit,
        round(
            sum(gross_profit) / nullif(sum(sale_price), 0),
            4
        )                                       as gross_margin_pct,

        sum(case when is_returned then 1 else 0 end)    as returned_units,
        round(
            sum(case when is_returned then 1 else 0 end)::float
            / nullif(count(order_item_id), 0),
            4
        )                                       as return_rate

    from order_items
    group by 1, 2, 3
)

select * from monthly_category
order by month, revenue desc
