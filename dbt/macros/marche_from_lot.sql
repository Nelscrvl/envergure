{#
  Marché d'un dossier ou d'un RDV, calculé une seule fois en staging.

  Par défaut : premier mot du lot normalisé ("UES IDF 22 - 16900" -> UES).
  seed_marche corrige les premiers mots qui ne sont pas des marchés : la clé est
  soit le lot complet (prioritaire), soit le premier mot, comparés sans casse.
  Les lots EMD avec région collée (EMD4IDF, EMD2NA...) relèvent tous du marché EMD.

  Usage : {{ marche_from_lot('l', 'm_lot', 'm_mot') }} avec
    l      = expression du lot normalisé
    m_lot  = alias de seed_marche joint sur le lot complet
    m_mot  = alias de seed_marche joint sur le premier mot
#}
{% macro premier_mot_lot(lot) -%}
SPLIT({{ lot }}, ' ')[SAFE_OFFSET(0)]
{%- endmacro %}

{% macro marche_from_lot(lot, m_lot, m_mot) -%}
COALESCE(
  {{ m_lot }}.marche,
  {{ m_mot }}.marche,
  IF(REGEXP_CONTAINS({{ premier_mot_lot(lot) }}, r'^EMD\d'), 'EMD', NULL),
  {{ premier_mot_lot(lot) }}
)
{%- endmacro %}
