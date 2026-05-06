{# Les deux sources (BDD_BU et BDD_EDM) contiennent toutes les deux des lignes TYPE=REALISE
   et TYPE=BUDGET pour les mêmes périmètres. Pour éviter un doublon lors du UNION ALL,
   on met la VALEUR à 0 dans BDD_BU pour ces deux types : seule BDD_EDM fait autorité sur
   REALISE et BUDGET. BDD_BU ne contribue que les types restants (ex: PREVISION, OBJECTIF). #}

WITH UNION_TABLE AS (
SELECT

*EXCEPT(VALEUR),
-- Anti-doublon : REALISE et BUDGET sont portés par BDD_EDM uniquement (cf. commentaire en-tête).
CASE WHEN TYPE = "REALISE" OR TYPE = "BUDGET" THEN 0
    ELSE VALEUR
END AS VALEUR


FROM {{ref("stg_DAF_BDD_BU_NATIVE")}}

UNION ALL

SELECT

*

FROM {{ref("stg_DAF_BDD_EDM_NATIVE")}}
)

SELECT 
*,
CASE WHEN ANNEE = "2025" THEN FORMAT_DATE('%Y-%m', DATE_ADD(PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT('%04d-%02d', CAST(ANNEE AS INT64), CAST(MOIS AS INT64)), '-01')), INTERVAL 1 YEAR)) 
ELSE ""
END AS periode_n1,
CASE WHEN CODE_BU LIKE "B%" THEN 1 
ELSE 0 
END AS CAT_BU_LIBELLE
FROM UNION_TABLE
