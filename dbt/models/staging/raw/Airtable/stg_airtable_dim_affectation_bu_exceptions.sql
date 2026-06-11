-- Staging Airtable : dim_affectation_bu_exceptions
-- Source : 0_airtable_raw.dim_affectation_bu_exceptions (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_affectation_bu_exceptions') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `Mois_debut` as mois_debut,
        `Pourcentage` as pourcentage,
        `BU_principale`[safe_offset(0)] as bu_principale,  -- lien forward Airtable aplati
        `BU_secondaire`[safe_offset(0)] as bu_secondaire,  -- lien forward Airtable aplati
        `Cle` as cle,
        `Created_By` as created_by,
        `Agence`[safe_offset(0)] as agence,  -- lien forward Airtable aplati
        `Mois_fin` as mois_fin,
        `Annee` as annee,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
