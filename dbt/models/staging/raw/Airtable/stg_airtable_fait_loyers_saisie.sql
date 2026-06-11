-- Staging Airtable : fait_loyers_saisie
-- Source : 0_airtable_raw.fait_loyers_saisie (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'fait_loyers_saisie') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `Juin` as juin,
        `Mars` as mars,
        `Agence`[safe_offset(0)] as agence,  -- lien forward Airtable aplati
        `Fevr` as fevr,
        `Total_annuel` as total_annuel,
        `Janv` as janv,
        `Annee` as annee,
        `Avr` as avr,
        `Mai` as mai,
        `Juil` as juil,
        `Cle` as cle,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
