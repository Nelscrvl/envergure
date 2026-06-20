with source as (
    select * from {{ source('teo', 'activity') }}
),

renamed as (
    select
        -- Identifiants
        id,
        action_id,
        cast(action__id as string)                  as action_internal_id,
        cast(action__type as string)                as action_type,
        cast(action_name as string)                 as action_name,

        -- Intervenant
        consultant_id,
        cast(consultant__id as string)              as consultant_internal_id,
        cast(consultant__type as string)            as consultant_type,
        cast(consultant_firstName as string)        as consultant_first_name,
        cast(consultant_lastName as string)         as consultant_last_name,
        cast(consultant_email as string)            as consultant_email,

        -- Lieu
        location_id,
        cast(location__id as string)                as location_internal_id,
        cast(location__type as string)              as location_type,
        cast(location_name as string)               as location_name,

        -- Séance
        cast(startAt as timestamp)                  as start_at,
        cast(duration as int64)                     as duration_minutes,
        cast(limitDossiers as int64)                as limit_dossiers,
        cast(description as string)                 as description,

        -- Participants (JSON array sérialisé)
        cast(dossiers as string)                    as dossiers_json,

        -- Flags
        cast(isCancelled as bool)                   as is_cancelled,
        cast(isDistancial as bool)                  as is_distancial,
        cast(isFull as bool)                        as is_full

    from source
)

select * from renamed
