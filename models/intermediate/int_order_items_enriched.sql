/*
  int_order_items_enriched
  ------------------------
  Enriches each order line item with product catalogue and inventory cost data.
  This is the core "fact" grain used by most downstream mart models.
*/

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

inventory as (
    select * from {{ ref('stg_inventory_items') }}
),

int_order_items_enriched as (
    select
        -- line item identifiers
        oi.order_item_id,
        oi.order_id,
        oi.user_id,
        oi.product_id,
        oi.inventory_item_id,

        -- order item status
        oi.status,
        oi.is_returned,
        oi.days_to_deliver,

        -- product attributes (from catalogue)
        p.product_name,
        p.product_category,
        p.product_brand,
        p.product_department,
        p.product_sku,

        -- financials
        oi.sale_price,
        coalesce(inv.cost, p.cost)              as unit_cost,
        oi.sale_price - coalesce(inv.cost, p.cost)  as gross_profit,
        round(
            case when oi.sale_price > 0
                 then (oi.sale_price - coalesce(inv.cost, p.cost)) / oi.sale_price
                 else null
            end,
            4
        )                                       as gross_margin_pct,

        -- timestamps
        oi.created_at,
        oi.shipped_at,
        oi.delivered_at,
        oi.returned_at

    from order_items oi
    left join products p
        on oi.product_id = p.product_id
    left join inventory inv
        on oi.inventory_item_id = inv.inventory_item_id
)

select * from int_order_items_enriched
