with users as (
    select * from {{ source('MD_Ecomm', 'users') }}
),

stg_users as (
    select
        -- ids
        id              as user_id,

        -- personal attributes
        first_name,
        last_name,
        first_name || ' ' || last_name  as full_name,
        email,
        age,
        gender,

        -- location
        city,
        state,
        country,
        postal_code,
        street_address,
        latitude,
        longitude,

        -- acquisition
        traffic_source,

        -- timestamps
        created_at

    from users
)

select * from stg_users
