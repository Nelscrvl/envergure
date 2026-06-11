-- Intermediate Airtable : dim_sites_boussole résolu
-- Part de stg_airtable_dim_sites_boussole, résout les liens forward (ID rec...) en libellés
-- par jointure sur airtable_id. Colonnes lookups/scalaires conservées.

with base as (
    select * from {{ ref('stg_airtable_dim_sites_boussole') }}
),

j_dim_territoires_su as (
    select airtable_id, code_se_bu_su from {{ ref('stg_airtable_dim_territoires_su') }}
)

select
    base.* except (dim_territoires_su),
    j_dim_territoires_su.code_se_bu_su as dim_territoires_su,  -- résolu -> dim_territoires_su
from base
left join j_dim_territoires_su on base.dim_territoires_su = j_dim_territoires_su.airtable_id
