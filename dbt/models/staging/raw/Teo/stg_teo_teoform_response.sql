with source as (
    select * from {{ source('teo', 'teoformResponse') }}
),

renamed as (
    select
        -- Identifiants réponse
        id,
        cast(externalId as string)                          as external_id,
        cast(createdAt as timestamp)                        as created_at,
        cast(updatedAt as timestamp)                        as updated_at,

        -- Contenu de la réponse (JSON sérialisé)
        cast(elements as string)                            as elements_json,

        -- Template associé
        cast(teoformTemplate_id as string)                  as template_id,
        cast(teoformTemplate__id as string)                 as template_internal_id,
        cast(teoformTemplate__type as string)               as template_type,
        cast(teoformTemplate_iid as int64)                  as template_iid,
        cast(teoformTemplate_title as string)               as template_title,
        cast(teoformTemplate_author as string)              as template_author,
        cast(teoformTemplate_titleColor as string)          as template_title_color,
        cast(teoformTemplate_subtitleColor as string)       as template_subtitle_color,
        cast(teoformTemplate_instructionColor as string)    as template_instruction_color,
        cast(teoformTemplate_active as bool)                as template_active,
        cast(teoformTemplate_private as bool)               as template_private,
        cast(teoformTemplate_createdAt as timestamp)        as template_created_at,
        cast(teoformTemplate_updatedAt as timestamp)        as template_updated_at,
        cast(teoformTemplate_elements as string)            as template_elements_json

    from source
)

select * from renamed
