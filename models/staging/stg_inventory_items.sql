with inventory_items as (
    select * from {{ source('Ecomm', 'inventory_items') }}
),

stg_inventory_items as (
    select
        -- ids
        id                          as inventory_item_id,
        product_id,
        product_distribution_center_id,

        -- product snapshot (denormalised on ingest from BigQuery export)
        product_name,
        product_brand,
        product_category,
        product_department,
        product_sku,
        product_retail_price,
        cost,

        -- derived
        case
            when sold_at is not null then true
            else false
        end                         as is_sold,

        -- timestamps
        created_at,
        sold_at

    from inventory_items
)

select * from stg_inventory_items
