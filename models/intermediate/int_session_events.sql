/*
  int_session_events
  ------------------
  Rolls raw web events up to session level. Computes session duration,
  event-type counts, and whether the session resulted in a purchase.
*/

with events as (
    select * from {{ ref('stg_events') }}
),

session_agg as (
    select
        session_id,
        user_id,
        traffic_source,
        browser,
        city,
        state,

        count(event_id)                             as total_events,
        min(created_at)                             as session_start_at,
        max(created_at)                             as session_end_at,
        date_diff('minute',
            min(created_at)::timestamp,
            max(created_at)::timestamp
        )                                           as session_duration_minutes,

        -- funnel steps
        sum(case when event_type = 'home'       then 1 else 0 end) as home_events,
        sum(case when event_type = 'department' then 1 else 0 end) as department_events,
        sum(case when event_type = 'product'    then 1 else 0 end) as product_views,
        sum(case when event_type = 'cart'       then 1 else 0 end) as cart_events,
        sum(case when event_type = 'purchase'   then 1 else 0 end) as purchase_events,
        sum(case when event_type = 'cancel'     then 1 else 0 end) as cancel_events,

        case when sum(case when event_type = 'purchase' then 1 else 0 end) > 0
             then true else false
        end                                         as converted

    from events
    group by 1, 2, 3, 4, 5, 6
)

select * from session_agg
