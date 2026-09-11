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
        convention_id_societe,
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
),

-- Seances realisees uniquement : Sofia contient aussi des seances planifiees
-- a venir, qui gonfleraient un realise qui n'a pas encore eu lieu.
realisees AS (
    SELECT * FROM seances WHERE date_seance <= CURRENT_DATE()
),

-- Heures conventionnees du groupe : places prevues x duree par stagiaire, soit
-- le volume si le groupe etait rempli a 100 %. Reprise telle quelle de
-- int_tarif_convention pour rester identique a mrt_inscrit_pilotage.
conventionne AS (
    SELECT id_convention, id_societe, heures_conventionnees_bdc
    FROM {{ ref('int_tarif_convention') }}
),

-- Interne / externe : champ para_sal_1 de la fiche Intervenant.
intervenants AS (
    SELECT CAST(intervenant_id AS STRING) AS intervenant_id, '2' AS id_societe, est_formateur_externe
    FROM {{ ref('stg_intervenant_Soc_2') }}
    UNION ALL
    SELECT CAST(intervenant_id AS STRING), '3', est_formateur_externe FROM {{ ref('stg_intervenant_Soc_3') }}
    UNION ALL
    SELECT CAST(intervenant_id AS STRING), '4', est_formateur_externe FROM {{ ref('stg_intervenant_Soc_4') }}
),

-- Tarif principal par convention. Meme regle de selection que Int_inscrit_formation
-- (est_principal d'abord, sinon le premier tarif).
tarifs AS (
    SELECT
        id_convention,
        id_societe,
        nom_type_tarif,
        SAFE_CAST(tt_prix                  AS NUMERIC) AS tt_prix,
        SAFE_CAST(tt_prix_stagiaire_centre AS NUMERIC) AS prix_stagiaire_centre,
        SAFE_CAST(tt_duree_prevue_heures   AS NUMERIC) AS duree_prevue_heures
    FROM {{ ref('stg_tarif_convention') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_convention, id_societe
        ORDER BY CASE WHEN UPPER(est_principal) = 'TRUE' THEN 0 ELSE 1 END,
                 id_tarif_convention
    ) = 1
)

SELECT
    -- Identifiants
    r.stagiaire_id,
    r.id_action,
    r.id_societe,
    r.code_analytique_parcours,
    r.libelle_court_parcours                        AS libelle_parcours,
    r.type_region,
    r.convention_id                                 AS id_convention,
    r.convention_numero_financeur                   AS conv_numero_financeur,
    r.client_nom                                    AS conv_client_nom,

    -- Dimension temporelle — le point de tout ce modèle
    r.date_seance,
    DATE_TRUNC(r.date_seance, MONTH)                AS mois_seance,
    EXTRACT(YEAR    FROM r.date_seance)             AS annee,
    EXTRACT(MONTH   FROM r.date_seance)             AS mois,
    EXTRACT(ISOWEEK FROM r.date_seance)             AS semaine,
    r.heure_debut,

    -- Séance
    r.libelle_type_seance,
    r.distancielle,
    r.type_absence_libelle,
    r.intervenant_id,
    r.formateur_nom_complet,
    COALESCE(iv.est_formateur_externe, FALSE)       AS est_formateur_externe,

    -- Heures stagiaire : heures_seance inclut l'absence, cohérent avec Int_inscrit_formation
    r.heures_seance,
    r.heures_absence,
    r.heures_seance - r.heures_absence              AS heures_effectives,
    IF(r.heures_absence > 0, 1, 0)                  AS est_absent,

    -- Heures formateur : une séance est suivie par N stagiaires, donc portée par N
    -- lignes ici. L'heure formateur n'est portée que par une seule d'entre elles,
    -- sinon SUM() multiplierait par l'effectif (106 720 h au lieu de 13 650 h).
    -- Côté formateur, utiliser SUM(heures_formateur), jamais SUM(heures_seance).
    IF(ROW_NUMBER() OVER (
           PARTITION BY r.intervenant_id, r.date_seance, r.heure_debut
           ORDER BY r.stagiaire_id
       ) = 1, r.heures_seance, 0)                   AS heures_formateur,

    -- CA à la séance : seul « Heure par stagiaire » se décompose ainsi. Les forfaits
    -- (stagiaire / groupe / montant global) sont des montants fixes non ventilables
    -- à la séance, donc NULL plutôt qu'une répartition arbitraire.
    t.nom_type_tarif,
    t.prix_stagiaire_centre,

    -- Attribut de convention : porte par une seule ligne pour rester sommable.
    IF(ROW_NUMBER() OVER (
           PARTITION BY r.convention_id, r.convention_id_societe
           ORDER BY r.date_seance, r.stagiaire_id, r.heure_debut
       ) = 1, c.heures_conventionnees_bdc, NULL)    AS heures_conventionnees_groupe,
    IF(t.nom_type_tarif = 'Heure par stagiaire',
       r.heures_seance * t.prix_stagiaire_centre,
       NULL)                                        AS ca_heure_realisee,

    -- CA forfaitaire : defini au niveau de l'inscription, pas de la seance. Porte
    -- par une seule ligne par (stagiaire x action), sinon SUM() le multiplierait
    -- par le nombre de seances. Renseigne uniquement pour le "Forfait stagiaire".
    --
    -- Pas de heures_conventionnees ici : le metier les definit comme
    -- places prevues x duree, soit un attribut de convention. La colonne vit dans
    -- mrt_inscrit_pilotage sous le nom heures_conventionnees_groupe. La calculer
    -- ici sur les inscrits donnerait 126 h la ou la convention en prevoit 168.
    IF(ROW_NUMBER() OVER (
           PARTITION BY r.stagiaire_id, r.id_action
           ORDER BY r.date_seance, r.heure_debut
       ) = 1, t.tt_prix, NULL)                      AS ca_forfait_stagiaire,

    -- Colonne unique a sommer cote CA : les deux sources sont exclusives par
    -- type de tarif, donc pas de double comptage.
    COALESCE(
        IF(t.nom_type_tarif = 'Heure par stagiaire',
           r.heures_seance * t.prix_stagiaire_centre, NULL),
        IF(ROW_NUMBER() OVER (
               PARTITION BY r.stagiaire_id, r.id_action
               ORDER BY r.date_seance, r.heure_debut
           ) = 1, t.tt_prix, NULL)
    )                                               AS ca_realise

FROM realisees r
LEFT JOIN intervenants iv
       ON  iv.intervenant_id = r.intervenant_id
       AND iv.id_societe     = r.id_societe
LEFT JOIN tarifs t
       ON  t.id_convention = CAST(r.convention_id         AS STRING)
       AND t.id_societe    = CAST(r.convention_id_societe AS STRING)
LEFT JOIN conventionne c
       ON  c.id_convention = CAST(r.convention_id         AS STRING)
       AND c.id_societe    = CAST(r.convention_id_societe AS STRING)
