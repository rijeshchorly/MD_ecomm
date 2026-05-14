/*
  int_user_sessions
  -----------------
  Rolls session-level web behaviour up to the user grain and joins it with
  lifetime purchase history. Bridges the web-events and order domains so
  downstream marts can compare acquisition source against customer value.
*/

with sessions as (
    select * from {{ ref('int_session_events') }}
    where user_id is not null
),

user_orders as (
    select * from {{ ref('int_user_order_history') }}
),

session_agg as (
    select
        user_id,

        count(session_id)                           as total_sessions,
        sum(total_events)                           as total_events,
        sum(product_views)                          as total_product_views,
        sum(cart_events)                            as total_cart_events,
        sum(purchase_events)                        as total_purchase_events,
        sum(cancel_events)                          as total_cancel_events,

        sum(case when converted then 1 else 0 end)  as converted_sessions,
        round(
            sum(case when converted then 1 else 0 end)::float
            / nullif(count(session_id), 0),
            4
        )                                           as session_conversion_rate,

        round(avg(session_duration_minutes), 2)     as avg_session_duration_minutes,
        sum(session_duration_minutes)               as total_session_minutes,

        min(session_start_at)                       as first_session_at,
        max(session_end_at)                         as last_session_at,

        -- modal traffic source / browser per user
        mode() within group (order by traffic_source) as primary_traffic_source,
        mode() within group (order by browser)        as primary_browser

    from sessions
    group by 1
),

final as (
    select
        sa.user_id,

        -- web behaviour
        sa.total_sessions,
        sa.total_events,
        sa.total_product_views,
        sa.total_cart_events,
        sa.total_purchase_events,
        sa.total_cancel_events,
        sa.converted_sessions,
        sa.session_conversion_rate,
        sa.avg_session_duration_minutes,
        sa.total_session_minutes,
        sa.first_session_at,
        sa.last_session_at,
        sa.primary_traffic_source,
        sa.primary_browser,

        -- purchase history (nulls for users with no order record)
        uo.lifetime_orders,
        uo.lifetime_revenue,
        uo.lifetime_gross_profit,
        uo.avg_order_value,
        uo.first_order_at,
        uo.most_recent_order_at,
        uo.customer_segment,

        -- web → purchase bridge
        case
            when uo.first_order_at is not null and sa.first_session_at is not null
            then date_diff('day',
                sa.first_session_at::date,
                uo.first_order_at::date
            )
        end                                         as days_first_session_to_first_order,

        case
            when uo.first_order_at is not null
            then (
                select count(*)
                from sessions s
                where s.user_id = sa.user_id
                  and s.session_start_at < uo.first_order_at
            )
        end                                         as sessions_before_first_order

    from session_agg sa
    left join user_orders uo
        on sa.user_id = uo.user_id
)

select * from final
