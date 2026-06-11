-- Staging Airtable : dim_rh
-- Source : 0_airtable_raw.dim_rh (base "Dimensions", chargée par extract_airtable.py)
-- Vue 1:1 : renommage snake_case + aplatissement des liens forward (ARRAY -> safe_offset(0)).
-- Résolution des rec... -> intermediate.

with source as (

    select * from {{ source('airtable_raw', 'dim_rh') }}

),

renamed as (

    select
        `_airtable_id` as airtable_id,
        `Date_entree` as date_entree,
        `NOM_PRENOM` as nom_prenom,
        `NOM` as nom,
        `Field_7` as field_7,
        `Taux_de_charge` as taux_de_charge,
        `Salaire_base` as salaire_base,
        `quotite_de_travail` as quotite_de_travail,
        `Mail` as mail,
        `STRUCTURE` as structure,
        `Prenom` as prenom,
        `MATRICULE` as matricule,
        `Tx_horaire` as tx_horaire,
        `_airtable_created_time` as airtable_created_time,
        `_extracted_at` as extracted_at
    from source

)

select * from renamed
