{{ config(schema='2_intermediate') }}

{# CA mandataire Envergure (book_2026 filtré sur type='FDG') — long format.
   Mapping rubriques : C625 (total A+F+P) + C628 (A) / C629 (F) / C630 (P).
   Règle métier : C625 (total) exclut les codifs famille D (pas de code mandat D)
   → garantit C625 = C628 + C629 + C630. #}

{%- set months = [
  'janvier','fevrier','mars','avril','mai','juin',
  'juillet','aout','septembre','octobre','novembre','decembre'
] -%}

WITH src AS (
  SELECT
    codification_action,
    analytique_bu                                  AS bu,
    COALESCE(NULLIF(UPPER(TRIM(famille_prestation)), ''), 'A')  AS famille,
    {%- for m in months %}
    {{ clean_numeric('ca_' ~ m) }} AS ca_{{ m }}{{ ',' if not loop.last }}
    {%- endfor %}
  FROM {{ source('DAF_Sources', 'book_2026') }}
  WHERE codification_action IS NOT NULL
    AND type = 'FDG'
),

unpiv AS (
  {%- for m in months %}
  SELECT codification_action, bu, famille, {{ loop.index }} AS mois, ca_{{ m }} AS valeur FROM src
  {{ 'UNION ALL' if not loop.last }}
  {%- endfor %}
),

agg AS (
  SELECT codification_action, bu, mois, famille, SUM(valeur) AS valeur
  FROM unpiv
  GROUP BY codification_action, bu, mois, famille
),

expanded AS (
  {{ expand_rubriques('agg', {
    'TOTAL': 'C625',
    'A':     'C628',
    'F':     'C629',
    'P':     'C630'
  }) }}
)

SELECT * FROM expanded
WHERE NOT (code_rubrique = 'C625' AND famille = 'D')
  AND valeur IS NOT NULL AND valeur != 0
