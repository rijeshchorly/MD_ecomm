with order_items as (
    select * from {{ source('Ecomm', 'order_items') }}
),

stg_order_items as (
    select
        -- ids
        id                  as order_item_id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,

        -- attributes
        status,
        sale_price,

        -- timestamps
        created_at,
        shipped_at,
        delivered_at,
        returned_at,

        -- derived
        case
            when returned_at is not null then true
            else false
        end                 as is_returned,

        case
            when delivered_at is not null
             and shipped_at   is not null
            then date_diff('day', shipped_at::date, delivered_at::date)
        end                 as days_to_deliver

    from order_items
)

select * from stg_order_items
