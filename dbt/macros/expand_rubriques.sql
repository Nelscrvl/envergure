{# Génère le SELECT final d'un intermediate : croise une CTE agrégée
   (codification_action, bu, mois, famille, valeur) avec un mapping
   {scope → code_rubrique}, et émet une ligne par couple `(input × scope_match)`.
   Le scope 'TOTAL' matche toujours ; les autres scopes (A/F/P/D) ne matchent
   que si la famille de la ligne agg correspond.

   Args:
     agg_ref : nom de la CTE source (string, ex: 'agg')
     mapping : dict {scope: code_rubrique}, ex: {'TOTAL':'C110','A':'C115',...}

   Exemple :
     {{ expand_rubriques('agg', {'TOTAL':'C110', 'A':'C115', 'F':'C116'}) }} #}
{% macro expand_rubriques(agg_ref, mapping) -%}
SELECT
  a.codification_action,
  a.bu,
  a.mois,
  a.famille,
  rm.code_rubrique,
  a.valeur
FROM {{ agg_ref }} a
INNER JOIN (
  {%- for scope, code in mapping.items() %}
  SELECT '{{ scope }}' AS scope, '{{ code }}' AS code_rubrique{{ ' UNION ALL' if not loop.last }}
  {%- endfor %}
) rm
  ON rm.scope = 'TOTAL'
  OR rm.scope = a.famille
{%- endmacro %}
