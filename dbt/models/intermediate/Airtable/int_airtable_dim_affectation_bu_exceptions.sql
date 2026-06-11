-- Intermediate Airtable : dim_affectation_bu_exceptions résolu
-- Part de stg_airtable_dim_affectation_bu_exceptions, résout les liens forward (ID rec...) en libellés
-- par jointure sur airtable_id. Colonnes lookups/scalaires conservées.

with base as (
    select * from {{ ref('stg_airtable_dim_affectation_bu_exceptions') }}
),

j_bu_secondaire as (
    select airtable_id, code_se_bu_su from {{ ref('stg_airtable_dim_territoires_su') }}
),

j_agence as (
    select airtable_id, nom from {{ ref('stg_airtable_dim_agences') }}
)

select
    base.* except (bu_secondaire, agence),
    j_bu_secondaire.code_se_bu_su as bu_secondaire,  -- résolu -> dim_territoires_su
    j_agence.nom as agence,  -- résolu -> dim_agences
from base
left join j_bu_secondaire on base.bu_secondaire = j_bu_secondaire.airtable_id
left join j_agence on base.agence = j_agence.airtable_id
