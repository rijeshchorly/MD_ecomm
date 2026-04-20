with orders as (
    select * from {{ source('MD_Ecomm', 'orders') }}
),

stg_orders as (
    select
        -- ids
        order_id,
        user_id,

        -- attributes
        status,
        gender,
        num_of_item,

        -- timestamps
        created_at,
        returned_at,
        shipped_at,
        delivered_at

    from orders
)

select * from stg_orders
