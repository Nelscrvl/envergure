{{ config(schema='2_mart') }}

{# Consolidation MS prod : ENV (CUBA) + ESO (BEN17).
   Long format avec code_rubrique (C119 total + C120/C121 par famille). #}

WITH unioned AS (
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('ENV_MS_PROD') }}
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('ESO_MS_PROD') }}
),

agg AS (
  SELECT
    codification_action,
    bu,
    mois,
    ANY_VALUE(famille) AS famille,
    code_rubrique,
    SUM(valeur)        AS valeur
  FROM unioned
  GROUP BY codification_action, bu, mois, code_rubrique
)

SELECT * FROM agg
WHERE valeur IS NOT NULL AND valeur != 0