-- Intermediate Airtable : fait_loyers_saisie résolu
-- Part de stg_airtable_fait_loyers_saisie, résout les liens forward (ID rec...) en libellés
-- par jointure sur airtable_id. Colonnes lookups/scalaires conservées.

with base as (
    select * from {{ ref('stg_airtable_fait_loyers_saisie') }}
),

j_agence as (
    select airtable_id, nom from {{ ref('stg_airtable_dim_agences') }}
)

select
    base.* except (agence),
    j_agence.nom as agence,  -- résolu -> dim_agences
from base
left join j_agence on base.agence = j_agence.airtable_id
