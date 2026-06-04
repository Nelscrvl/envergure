WITH union_presence AS (
    SELECT * FROM {{ ref("stg_presence_Soc_2") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_presence_Soc_3") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_presence_Soc_4") }}
)

SELECT
    intervenant_id,
    intervenant_reference,
    intervenant_nom,
    intervenant_prenom,
    id_action,
    libelle_action,
    code_analytique_parcours,
    SUM(TIME_DIFF(duree_seance, TIME(0, 0, 0), MINUTE)) / 60.0 AS heures_formateur
FROM union_presence
WHERE intervenant_id IS NOT NULL
GROUP BY
    intervenant_id,
    intervenant_reference,
    intervenant_nom,
    intervenant_prenom,
    id_action,
    libelle_action,
    code_analytique_parcours
