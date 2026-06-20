with source as (
    select * from {{ source('teo', 'externalLink') }}
),

renamed as (
    select
        -- Identifiants
        id,
        cast(internalId as string)                  as internal_id,
        cast(externalId as string)                  as external_id,

        -- Connecteur (système externe lié)
        connector_id,
        cast(connector_type as string)              as connector_type,
        cast(connector__id as string)               as connector_internal_id,
        cast(connector__type as string)             as connector_internal_type

    from source
)

select * from renamed
