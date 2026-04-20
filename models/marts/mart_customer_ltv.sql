/*
  mart_customer_ltv
  -----------------
  One row per customer with lifetime value metrics, demographics, and
  acquisition source. The primary customer analytics mart.
*/

with user_history as (
    select * from {{ ref('int_user_order_history') }}
)

select
    user_id,
    full_name,
    email,
    age,
    gender,
    city,
    state,
    country,
    traffic_source,
    user_created_at,

    -- purchase behaviour
    customer_segment,
    lifetime_orders,
    lifetime_revenue,
    lifetime_gross_profit,
    avg_order_value,
    total_returned_items,

    -- activity window
    first_order_at,
    most_recent_order_at,
    days_customer_active,

    -- LTV tiers (simple revenue-based bucketing)
    case
        when lifetime_revenue = 0         then 'no_spend'
        when lifetime_revenue < 100       then 'low'
        when lifetime_revenue < 500       then 'mid'
        when lifetime_revenue < 1000      then 'high'
        else                                   'vip'
    end                                         as ltv_tier,

    -- return rate
    round(
        total_returned_items::float / nullif(lifetime_orders, 0),
        4
    )                                           as return_rate_per_order

from user_history
