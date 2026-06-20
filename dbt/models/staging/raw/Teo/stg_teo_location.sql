with source as (
    select * from {{ source('teo', 'location') }}
),

renamed as (
    select
        -- Identifiants
        id,
        cast(name as string)                        as name,

        -- Adresse
        cast(address__id as string)                 as address_internal_id,
        cast(address__type as string)               as address_type,
        cast(address_streetAddress as string)       as address_street,
        cast(address_city as string)                as address_city,
        cast(address_postalCode as int64)           as address_postal_code

    from source
)

select * from renamed
