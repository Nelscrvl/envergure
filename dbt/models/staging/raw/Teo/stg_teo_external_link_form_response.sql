with source as (
    select * from {{ source('teo', 'externalLinkFormResponse') }}
),

renamed as (
    select
        -- Identifiants
        id,
        cast(externalId as string)                  as external_id,
        cast(formTemplateId as string)              as form_template_id,

        -- Contenu (JSON sérialisé)
        cast(elements as string)                    as elements_json,

        -- Horodatage
        cast(createdAt as timestamp)                as created_at,
        cast(updatedAt as timestamp)                as updated_at

    from source
)

select * from renamed
