{{ config(schema='2_mart') }}

{# Consolidation sous-traitance : ENV (CUBA) + ESO (BEN17).
   Long format avec code_rubrique (C124 total + C125/C126/C606 par famille). #}

WITH unioned AS (
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('ENV_SS_TRAITANT') }}
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('ESO_SS_TRAITANT') }}
)

SELECT
  codification_action,
  bu,
  mois,
  ANY_VALUE(famille) AS famille,
  code_rubrique,
  SUM(valeur)        AS valeur
FROM unioned
GROUP BY codification_action, bu, mois, code_rubrique
