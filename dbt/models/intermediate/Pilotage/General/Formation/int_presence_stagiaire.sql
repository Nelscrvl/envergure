{{ config(materialized='table') }}

-- Présences au grain séance, côté stagiaire.
-- Pendant stagiaire de int_presence_pilotage (qui est formateur-centrique).
-- Contrairement à int_inscrit_pilotage, la date de la séance est conservée :
-- une ligne = un stagiaire présent à une séance datée.

WITH union_presence AS (
    SELECT *, '2' AS id_societe FROM {{ ref('stg_presence_Soc_2') }}
    UNION ALL
    SELECT *, '3' AS id_societe FROM {{ ref('stg_presence_Soc_3') }}
    UNION ALL
    SELECT *, '4' AS id_societe FROM {{ ref('stg_presence_Soc_4') }}
),

-- Deux sources de doublons à écraser :
--   1. stg_presence_* fait un CROSS JOIN UNNEST(Intervenants) : n lignes si n intervenants
--      sur la séance (Soc_3 est à ~6x pour cette raison)
--   2. une même séance peut exister dans plusieurs sociétés (181 séances en 2+3+4)
-- Priorité Soc_2 > Soc_3 > Soc_4, cohérent avec dedup_inscrite dans Int_inscrit_formation.
seances AS (
    SELECT
        stagiaire_id,
        CAST(id_action AS STRING)                                       AS id_action,
        SAFE_CAST(LEFT(date_date, 10) AS DATE)                          AS date_seance,
        heure_debut,
        id_societe,
        code_analytique_parcours,
        libelle_court_parcours,
        type_region,
        convention_id,
        convention_numero_financeur,
        client_nom,
        libelle_type_seance,
        distancielle,
        type_absence_libelle,
        CAST(intervenant_id AS STRING)                                  AS intervenant_id,
        CONCAT(COALESCE(intervenant_nom, ''), ' ', COALESCE(intervenant_prenom, ''))
                                                                        AS formateur_nom_complet,
        TIME_DIFF(duree_seance, TIME(0, 0, 0), MINUTE) / 60.0           AS heures_seance,
        TIME_DIFF(COALESCE(duree_absence, TIME(0, 0, 0)), TIME(0, 0, 0), MINUTE) / 60.0
                                                                        AS heures_absence
    FROM union_presence
    WHERE stagiaire_id IS NOT NULL
      AND duree_seance IS NOT NULL
      AND SAFE_CAST(LEFT(date_date, 10) AS DATE) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stagiaire_id, CAST(id_action AS STRING), LEFT(date_date, 10), heure_debut
        ORDER BY CAST(id_societe AS INT64), intervenant_id
    ) = 1
)

SELECT
    -- Identifiants
    stagiaire_id,
    id_action,
    id_societe,
    code_analytique_parcours,
    libelle_court_parcours                          AS libelle_parcours,
    type_region,
    convention_id                                   AS id_convention,
    convention_numero_financeur                     AS conv_numero_financeur,
    client_nom                                      AS conv_client_nom,

    -- Dimension temporelle — le point de tout ce modèle
    date_seance,
    DATE_TRUNC(date_seance, MONTH)                  AS mois_seance,
    EXTRACT(YEAR    FROM date_seance)               AS annee,
    EXTRACT(MONTH   FROM date_seance)               AS mois,
    EXTRACT(ISOWEEK FROM date_seance)               AS semaine,
    heure_debut,

    -- Séance
    libelle_type_seance,
    distancielle,
    type_absence_libelle,
    intervenant_id,
    formateur_nom_complet,

    -- Heures : heures_seance inclut l'absence, cohérent avec Int_inscrit_formation
    heures_seance,
    heures_absence,
    heures_seance - heures_absence                  AS heures_effectives,
    IF(heures_absence > 0, 1, 0)                    AS est_absent
FROM seances
-- Séances réalisées uniquement : Sofia contient aussi des séances planifiées
-- à venir, qui gonfleraient les heures d'un réalisé qui n'a pas encore eu lieu.
WHERE date_seance <= CURRENT_DATE()
