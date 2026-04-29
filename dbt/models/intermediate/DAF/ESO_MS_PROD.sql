{{ config(schema='2_intermediate') }}

{# MS prod BEN17 (ESO) — long format avec code_rubrique.
   Source : `ms_prod_non_chargee` de BEN17_R2026.
   La valeur émise est la **MS Prod BRUTE** (sans chargement RH).
   Le chargement (× H02B_TAUX_CHARGES_SOCIALES) est appliqué côté Sheet
   `06_Calculs` du reporting v6, à partir de `1_natives.HYPOTHESES_2026`.
   Mapping rubriques : C119 (total A+F+P) + C120 (A) / C121 (F) / C611 (P).
   Pas de code MS Prod pour D → C119 exclut famille D pour assurer
   C119 = C120 + C121 + C611. #}

WITH r2026 AS (
  SELECT
    codif_action                          AS codification_action,
    SAFE_CAST(mois AS INT64)              AS mois,
    {{ clean_numeric('ms_prod_non_chargee') }} AS ms_prod_non_chargee
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
    SUM(r.ms_prod_non_chargee) AS valeur  -- MS Prod brute, chargement appliqué côté Sheet
  FROM r2026 r
  LEFT JOIN ref_marche m USING (codification_action)
  GROUP BY r.codification_action, m.bu, r.mois, m.famille
),

expanded AS (
  {{ expand_rubriques('agg', {
    'TOTAL': 'C119',
    'A':     'C120',
    'F':     'C121',
    'P':     'C611'
  }) }}
)

SELECT * FROM expanded
WHERE NOT (code_rubrique = 'C119' AND famille = 'D')
  AND valeur IS NOT NULL AND valeur != 0
