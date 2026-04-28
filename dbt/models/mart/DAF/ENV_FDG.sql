{{ config(schema='2_intermediate') }}

{# Frais de gestion Envergure (book_2026, colonnes mois_01..12) — long format.
   Mapping rubriques : C123 (total A+F+P) + C603 (A) / C604 (F) / C605 (P).
   Règle métier : C123 (total) exclut les codifs famille D (pas de code FdG D)
   → garantit C123 = C603 + C604 + C605. #}

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
    {{ clean_numeric('mois_' ~ '%02d' % loop.index) }} AS fdg_{{ m }}{{ ',' if not loop.last }}
    {%- endfor %}
  FROM {{ source('DAF_Sources', 'book_2026') }}
  WHERE codification_action IS NOT NULL
),

unpiv AS (
  {%- for m in months %}
  SELECT codification_action, bu, famille, {{ loop.index }} AS mois, fdg_{{ m }} AS valeur FROM src
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
    'TOTAL': 'C123',
    'A':     'C603',
    'F':     'C604',
    'P':     'C605'
  }) }}
)

SELECT * FROM expanded
WHERE NOT (code_rubrique = 'C123' AND famille = 'D')
  AND valeur IS NOT NULL AND valeur != 0
