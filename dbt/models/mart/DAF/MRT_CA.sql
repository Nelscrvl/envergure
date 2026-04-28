{{ config(schema='2_mart') }}

{# Consolidation CA : ENV (book_2026) + ESO (BEN17).
   Long format avec code_rubrique (C110 total + C115/C116/C601/C602 par famille).
   Sources disjointes aujourd'hui ; le GROUP BY final garantit l'unicité
   (codification_action, bu, mois, code_rubrique) et somme si recouvrement futur. #}

WITH unioned AS (
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('ENV_CA') }}
  UNION ALL
  SELECT codification_action, bu, mois, famille, code_rubrique, valeur FROM {{ ref('ESO_CA') }}
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
