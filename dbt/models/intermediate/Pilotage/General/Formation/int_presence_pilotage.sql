{{ config(materialized='table') }}

WITH union_presence AS (
    SELECT *, '2' AS id_societe FROM {{ ref('stg_presence_Soc_2') }}
    UNION ALL
    SELECT *, '3' AS id_societe FROM {{ ref('stg_presence_Soc_3') }}
    UNION ALL
    SELECT *, '4' AS id_societe FROM {{ ref('stg_presence_Soc_4') }}
),

intervenants AS (
    SELECT CAST(intervenant_id AS STRING) AS intervenant_id, '2' AS id_societe, est_formateur_externe
    FROM {{ ref('stg_intervenant_Soc_2') }}
    UNION ALL
    SELECT CAST(intervenant_id AS STRING) AS intervenant_id, '3' AS id_societe, est_formateur_externe
    FROM {{ ref('stg_intervenant_Soc_3') }}
    UNION ALL
    SELECT CAST(intervenant_id AS STRING) AS intervenant_id, '4' AS id_societe, est_formateur_externe
    FROM {{ ref('stg_intervenant_Soc_4') }}
),

-- Déduplication : une séance par (formateur × action × date × heure_debut)
sessions AS (
    SELECT DISTINCT
        CAST(intervenant_id AS STRING)  AS intervenant_id,
        id_action,
        SAFE_CAST(date_date AS DATE)    AS date_date,
        heure_debut,
        id_societe,
        code_analytique_parcours,
        TIME_DIFF(duree_seance, TIME(0, 0, 0), MINUTE) / 60.0 AS heures_seance
    FROM union_presence
    WHERE intervenant_id IS NOT NULL
      AND duree_seance IS NOT NULL
)

SELECT
    s.intervenant_id,
    s.id_action,
    s.code_analytique_parcours,
    s.date_date,
    EXTRACT(YEAR  FROM s.date_date)                          AS annee,
    EXTRACT(MONTH FROM s.date_date)                          AS mois,
    EXTRACT(ISOWEEK FROM s.date_date)                        AS semaine,
    s.heure_debut,
    s.id_societe,
    s.heures_seance,
    COALESCE(iv.est_formateur_externe, FALSE)                AS est_formateur_externe
FROM sessions s
LEFT JOIN intervenants iv
    ON  iv.intervenant_id = s.intervenant_id
    AND iv.id_societe     = s.id_societe
