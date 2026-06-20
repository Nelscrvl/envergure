with source as (
    select * from {{ source('teo', 'dossier') }}
),

renamed as (
    select
        -- Identifiants
        id,
        iid,
        cast(status as string)                      as status,

        -- Bénéficiaire
        cast(firstName as string)                   as first_name,
        cast(lastName as string)                    as last_name,
        cast(email as string)                       as email,
        cast(phone as string)                       as phone,
        cast(birthDate as date)                     as birth_date,

        -- Adresse
        cast(address as string)                     as address,
        cast(address__id as string)                 as address_internal_id,
        cast(address__type as string)               as address_type,
        cast(address_streetAddress as string)       as address_street,
        cast(address_city as string)                as address_city,
        cast(address_postalCode as int64)           as address_postal_code,

        -- Intervenant
        consultant_id,
        cast(consultant__id as string)              as consultant_internal_id,
        cast(consultant__type as string)            as consultant_type,
        cast(consultant_firstName as string)        as consultant_first_name,
        cast(consultant_lastName as string)         as consultant_last_name,
        cast(consultant_email as string)            as consultant_email,

        -- Site
        location_id,
        cast(location__id as string)                as location_internal_id,
        cast(location__type as string)              as location_type,
        cast(location_name as string)               as location_name,

        -- Service / prestation
        service_id,
        cast(service__id as string)                 as service_internal_id,
        cast(service__type as string)               as service_type,
        cast(service_name as string)                as service_name,

        -- Commande (order = contrat/mission)
        cast(order__id as string)                   as order_internal_id,
        cast(order__type as string)                 as order_type,
        cast(order_startDate as date)               as order_start_date,
        cast(order_endDate as date)                 as order_end_date

    from source
)

select * from renamed
