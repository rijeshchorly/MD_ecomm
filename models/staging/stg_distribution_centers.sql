with distribution_centers as (
    select * from {{ source('MD_Ecomm', 'distribution_centers') }}
),

stg_distribution_centers as (
    select
        -- ids
        id          as distribution_center_id,

        -- attributes
        name        as distribution_center_name,
        latitude    as dc_latitude,
        longitude   as dc_longitude

    from distribution_centers
)

select * from stg_distribution_centers
