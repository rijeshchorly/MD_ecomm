with products as (
    select * from {{ source('MD_Ecomm', 'products') }}
),

stg_products as (
    select
        -- ids
        id                          as product_id,

        -- attributes
        name                        as product_name,
        category                    as product_category,
        brand                       as product_brand,
        department                  as product_department,
        sku                         as product_sku,

        -- pricing
        retail_price,
        cost,
        round(retail_price - cost, 2)   as gross_margin,
        round(
            case when retail_price > 0
                 then (retail_price - cost) / retail_price
                 else null
            end,
            4
        )                               as gross_margin_pct,

        -- distribution
        distribution_center_id

    from products
)

select * from stg_products
