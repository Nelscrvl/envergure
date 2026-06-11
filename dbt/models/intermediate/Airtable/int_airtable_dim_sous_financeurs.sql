-- Intermediate Airtable : dim_sous_financeurs résolu
-- Part de stg_airtable_dim_sous_financeurs, résout les liens forward (ID rec...) en libellés
-- par jointure sur airtable_id. Colonnes lookups/scalaires conservées.

with base as (
    select * from {{ ref('stg_airtable_dim_sous_financeurs') }}
),

j_financeur_associe as (
    select airtable_id, financeur from {{ ref('stg_airtable_dim_financeurs') }}
)

select
    base.* except (financeur_associe),
    j_financeur_associe.financeur as financeur_associe,  -- résolu -> dim_financeurs
from base
left join j_financeur_associe on base.financeur_associe = j_financeur_associe.airtable_id
