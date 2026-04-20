/*
  int_user_order_history
  ----------------------
  Builds a per-user purchase history spine: lifetime revenue, order counts,
  first/last order dates, and recency. Used by the customer LTV mart.
*/

with users as (
    select * from {{ ref('stg_users') }}
),

orders as (
    select * from {{ ref('int_orders_with_metrics') }}
    where status not in ('Cancelled')
),

order_history as (
    select
        user_id,
        count(order_id)                             as lifetime_orders,
        sum(order_revenue)                          as lifetime_revenue,
        sum(order_gross_profit)                     as lifetime_gross_profit,
        round(avg(order_revenue), 2)                as avg_order_value,
        sum(returned_item_count)                    as total_returned_items,
        min(created_at)                             as first_order_at,
        max(created_at)                             as most_recent_order_at,
        date_diff('day',
            min(created_at)::date,
            max(created_at)::date
        )                                           as days_customer_active
    from orders
    group by 1
),

final as (
    select
        u.user_id,
        u.full_name,
        u.email,
        u.age,
        u.gender,
        u.city,
        u.state,
        u.country,
        u.traffic_source,
        u.created_at                                as user_created_at,

        -- order history (nulls if no orders yet)
        coalesce(oh.lifetime_orders, 0)             as lifetime_orders,
        coalesce(oh.lifetime_revenue, 0)            as lifetime_revenue,
        coalesce(oh.lifetime_gross_profit, 0)       as lifetime_gross_profit,
        oh.avg_order_value,
        coalesce(oh.total_returned_items, 0)        as total_returned_items,
        oh.first_order_at,
        oh.most_recent_order_at,
        oh.days_customer_active,

        -- customer segment (simple RFM-style bucket)
        case
            when oh.lifetime_orders is null             then 'no_orders'
            when oh.lifetime_orders = 1                 then 'one_time'
            when oh.lifetime_orders between 2 and 4     then 'repeat'
            else                                             'loyal'
        end                                         as customer_segment

    from users u
    left join order_history oh
        on u.user_id = oh.user_id
)

select * from final
