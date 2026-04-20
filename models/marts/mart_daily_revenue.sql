/*
  mart_daily_revenue
  ------------------
  Daily revenue, cost, and gross profit for completed and shipped orders.
  Great for time-series dashboards and trend analysis.
*/

with orders as (
    select * from {{ ref('int_orders_with_metrics') }}
    where status in ('Complete', 'Shipped')
),

daily as (
    select
        created_at::date                        as order_date,
        date_trunc('month', created_at)::date   as order_month,
        date_trunc('year',  created_at)::date   as order_year,

        count(order_id)                         as orders_placed,
        count(distinct user_id)                 as unique_buyers,
        sum(item_count)                         as total_items_sold,
        round(sum(order_revenue), 2)            as gross_revenue,
        round(sum(order_cost), 2)               as total_cost,
        round(sum(order_gross_profit), 2)       as gross_profit,
        round(avg(order_revenue), 2)            as avg_order_value,
        round(
            sum(order_gross_profit) / nullif(sum(order_revenue), 0),
            4
        )                                       as gross_margin_pct

    from orders
    group by 1, 2, 3
)

select * from daily
order by order_date
