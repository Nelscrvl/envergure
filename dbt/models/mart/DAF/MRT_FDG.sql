{{ config(schema='2_mart') }}

{# Consolidation FdG : ENV (book_2026) + ESO (BEN17).
   Long format avec code_rubrique (C123 total + C603/C604/C605 par famille). #}

WITH unioned AS (
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('ENV_FDG') }}
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('ESO_FDG') }}
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