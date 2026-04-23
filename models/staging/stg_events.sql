with events as (
    select * from {{ source('Ecomm', 'events') }}
),

stg_events as (
    select
        -- ids
        id              as event_id,
        user_id,
        session_id,
        sequence_number,

        -- event attributes
        event_type,
        uri,

        -- location & device
        city,
        state,
        postal_code,
        ip_address,
        browser,
        traffic_source,

        -- timestamps
        created_at

    from events
)

select * from stg_events
