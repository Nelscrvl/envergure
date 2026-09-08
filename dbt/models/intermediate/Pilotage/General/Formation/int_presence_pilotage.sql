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

-- Dimensions action depuis le modèle inscrit (dédupliquées par action)
dims_action AS (
    SELECT DISTINCT
        CAST(IDAction AS STRING)        AS id_action,
        CAST(conv_id_societe AS STRING) AS id_societe,
        Libelle_Court_Parcours          AS libelle_parcours,
        Type_Region                     AS type_region,
        conv_numero_financeur,
        conv_client_nom,
        nom_type_tarif
    FROM {{ ref('Int_inscrit_formation') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(IDAction AS STRING), CAST(conv_id_societe AS STRING) ORDER BY IDAction) = 1
),

-- Nom complet formateur depuis les intervenants
noms_intervenants AS (
    SELECT
        CAST(intervenant_id AS STRING) AS intervenant_id,
        '2' AS id_societe,
        CONCAT(COALESCE(Nom, ''), ' ', COALESCE(Prenom, '')) AS formateur_nom_complet
    FROM {{ ref('stg_intervenant_Soc_2') }}
    UNION ALL
    SELECT CAST(intervenant_id AS STRING), '3',
        CONCAT(COALESCE(Nom, ''), ' ', COALESCE(Prenom, ''))
    FROM {{ ref('stg_intervenant_Soc_3') }}
    UNION ALL
    SELECT CAST(intervenant_id AS STRING), '4',
        CONCAT(COALESCE(Nom, ''), ' ', COALESCE(Prenom, ''))
    FROM {{ ref('stg_intervenant_Soc_4') }}
),

-- Déduplication : une séance par (formateur × action × date × heure_debut)
sessions AS (
    SELECT DISTINCT
        CAST(intervenant_id AS STRING)  AS intervenant_id,
        CAST(id_action AS STRING)       AS id_action,
        SAFE_CAST(LEFT(date_date, 10) AS DATE) AS date_date,
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
    COALESCE(iv.est_formateur_externe, FALSE)                AS est_formateur_externe,
    ni.formateur_nom_complet,
    da.libelle_parcours,
    da.type_region,
    da.conv_numero_financeur,
    da.conv_client_nom,
    da.nom_type_tarif
FROM sessions s
LEFT JOIN intervenants iv
    ON  iv.intervenant_id = s.intervenant_id
    AND iv.id_societe     = s.id_societe
LEFT JOIN noms_intervenants ni
    ON  ni.intervenant_id = s.intervenant_id
    AND ni.id_societe     = s.id_societe
LEFT JOIN dims_action da
    ON  da.id_action  = s.id_action
    AND da.id_societe = s.id_societe
