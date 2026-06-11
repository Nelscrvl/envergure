-- Intermediate Airtable : fait_loyers_affectation résolu
-- Part de stg_airtable_fait_loyers_affectation, résout les liens forward (ID rec...) en libellés
-- par jointure sur airtable_id. Colonnes lookups/scalaires conservées.

with base as (
    select * from {{ ref('stg_airtable_fait_loyers_affectation') }}
),

j_bu as (
    select airtable_id, code_se_bu_su from {{ ref('stg_airtable_dim_territoires_su') }}
),

j_agence as (
    select airtable_id, nom from {{ ref('stg_airtable_dim_agences') }}
)

select
    base.* except (bu, agence),
    j_bu.code_se_bu_su as bu,  -- résolu -> dim_territoires_su
    j_agence.nom as agence,  -- résolu -> dim_agences
from base
left join j_bu on base.bu = j_bu.airtable_id
left join j_agence on base.agence = j_agence.airtable_id
