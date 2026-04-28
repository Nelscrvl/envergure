{{ config(schema='2_intermediate') }}

{# CA Envergure (book_2026) — long format avec code_rubrique.
   Mapping rubriques : C110 (total A+F+P uniquement) + C115 (A) / C116 (F) / C601 (P) / C602 (D).
   Règle métier : C110 (CHIFFRE D AFFAIRES) n'inclut PAS le CA Divers (C602)
   qui est traité comme une catégorie distincte. #}

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
    'TOTAL': 'C110',
    'A':     'C115',
    'F':     'C116',
    'P':     'C601',
    'D':     'C602'
  }) }}
)

SELECT * FROM expanded
-- C110 (CA total) exclut les codifs Divers — C602 reste leur seule rubrique
WHERE NOT (code_rubrique = 'C110' AND famille = 'D')
