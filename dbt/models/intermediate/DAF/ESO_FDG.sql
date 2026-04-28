{{ config(schema='2_intermediate') }}

{# Frais de gestion BEN17 (ESO) — long format avec code_rubrique.
   Mapping rubriques : C123 (total A+F+P) + C603 (A) / C604 (F) / C605 (P).
   Règle métier : C123 (total) exclut les codifs famille D (pas de code FdG D)
   pour garantir C123 = C603 + C604 + C605. #}

WITH r2026 AS (
  SELECT
    codif_action             AS codification_action,
    SAFE_CAST(mois AS INT64) AS mois,
    {{ clean_numeric('fdg') }} AS fdg
  FROM {{ source('DAF_Sources', 'BEN17_R2026') }}
  WHERE codif_action IS NOT NULL
),

ref_marche AS (
  SELECT
    Codif_action_site                              AS codification_action,
    Code_BU                                        AS bu,
    COALESCE(NULLIF(UPPER(TRIM(Activit__)), ''), 'A')           AS famille
  FROM {{ source('DAF_Sources', 'BEN17_REF_MARCHE') }}
  WHERE Codif_action_site IS NOT NULL
),

agg AS (
  SELECT
    r.codification_action,
    m.bu,
    r.mois,
    m.famille,
    SUM(r.fdg) AS valeur
  FROM r2026 r
  LEFT JOIN ref_marche m USING (codification_action)
  GROUP BY r.codification_action, m.bu, r.mois, m.famille
),

expanded AS (
  {{ expand_rubriques('agg', {
    'TOTAL': 'C123',
    'A':     'C603',
    'F':     'C604',
    'P':     'C605'
  }) }}
)

SELECT * FROM expanded
-- C123 (total) exclut famille D (pas de sous-rubrique → assure total = somme breakdown)
WHERE NOT (code_rubrique = 'C123' AND famille = 'D')
  AND valeur IS NOT NULL AND valeur != 0
