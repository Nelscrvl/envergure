-- Staging Airtable : fait_loyers_affectation
-- Source : 0_airtable_raw.fait_loyers_affectation (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'fait_loyers_affectation') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `BU`[safe_offset(0)] as bu,  -- lien forward Airtable aplati
        `Date_MAJ` as date_maj,
        `Montant_BU` as montant_bu,
        `Annee` as annee,
        `Montant_loyer` as montant_loyer,
        `Mois` as mois,
        `Pourcentage` as pourcentage,
        `Source_affectation` as source_affectation,
        `Agence`[safe_offset(0)] as agence,  -- lien forward Airtable aplati
        `Cle` as cle,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
