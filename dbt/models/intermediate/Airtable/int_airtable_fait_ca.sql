-- Intermediate Airtable : fait_ca résolu
-- Part de stg_airtable_fait_ca, résout les liens forward (ID rec...) en libellés
-- par jointure sur airtable_id. Colonnes lookups/scalaires conservées.

with base as (
    select * from {{ ref('stg_airtable_fait_ca') }}
),

j_agence_from_dim_adv as (
    select airtable_id, nom from {{ ref('stg_airtable_dim_agences') }}
),

j_dim_adv as (
    select airtable_id, codif_prestation from {{ ref('stg_airtable_dim_adv') }}
)

select
    base.* except (agence_from_dim_adv, dim_adv),
    j_agence_from_dim_adv.nom as agence_from_dim_adv,  -- résolu -> dim_agences
    j_dim_adv.codif_prestation as dim_adv,  -- résolu -> dim_adv
from base
left join j_agence_from_dim_adv on base.agence_from_dim_adv = j_agence_from_dim_adv.airtable_id
left join j_dim_adv on base.dim_adv = j_dim_adv.airtable_id
