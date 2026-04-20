/*
  int_orders_with_metrics
  -----------------------
  Rolls enriched order items up to order level, adding revenue and
  fulfilment metrics alongside order header attributes.
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items_enriched as (
    select * from {{ ref('int_order_items_enriched') }}
),

item_agg as (
    select
        order_id,
        count(*)                            as item_count,
        sum(sale_price)                     as order_revenue,
        sum(unit_cost)                      as order_cost,
        sum(gross_profit)                   as order_gross_profit,
        round(avg(gross_margin_pct), 4)     as avg_item_margin_pct,
        sum(case when is_returned then 1 else 0 end) as returned_item_count,
        min(created_at)                     as first_item_created_at,
        max(shipped_at)                     as last_shipped_at,
        max(delivered_at)                   as last_delivered_at
    from order_items_enriched
    group by 1
),

int_orders_with_metrics as (
    select
        -- header
        o.order_id,
        o.user_id,
        o.status,
        o.gender,
        o.num_of_item,

        -- item aggregates
        ia.item_count,
        ia.order_revenue,
        ia.order_cost,
        ia.order_gross_profit,
        ia.avg_item_margin_pct,
        ia.returned_item_count,
        case when ia.returned_item_count > 0 then true else false end as has_return,

        -- fulfilment
        ia.last_shipped_at,
        ia.last_delivered_at,
        case
            when ia.last_delivered_at is not null and o.created_at is not null
            then date_diff('day', o.created_at::date, ia.last_delivered_at::date)
        end                                             as days_order_to_delivery,

        -- timestamps
        o.created_at,
        o.returned_at,
        o.shipped_at,
        o.delivered_at

    from orders o
    left join item_agg ia
        on o.order_id = ia.order_id
)

select * from final
