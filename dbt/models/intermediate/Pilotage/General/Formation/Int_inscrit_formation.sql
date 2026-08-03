WITH union_inscrite AS (
    SELECT *, '2' AS id_societe FROM {{ ref("stg_inscrite_Soc_2") }}
    UNION ALL
    SELECT *, '3' AS id_societe FROM {{ ref("stg_inscrite_Soc_3") }}
    UNION ALL
    SELECT *, '4' AS id_societe FROM {{ ref("stg_inscrite_Soc_4") }}
),

dedup_inscrite AS (
    SELECT
        * EXCEPT(stg_adresse_internal_id, stg_stagiaire_internal_id),
        CASE WHEN fr_formateur_id IS NULL THEN 0 ELSE 1 END AS presence
    FROM union_inscrite
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY IDAction) = 1
),

union_presence AS (
    SELECT * FROM {{ ref("stg_presence_Soc_2") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_presence_Soc_3") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_presence_Soc_4") }}
),

heures_presence AS (
    SELECT
        stagiaire_id,
        id_action,
        SUM(TIME_DIFF(duree_seance,                            TIME(0, 0, 0), MINUTE)) / 60.0 AS heures_realisees,
        SUM(TIME_DIFF(COALESCE(duree_absence, TIME(0, 0, 0)), TIME(0, 0, 0), MINUTE)) / 60.0 AS heures_absence
    FROM (
        SELECT DISTINCT stagiaire_id, id_action, date_date, heure_debut, duree_seance, duree_absence
        FROM union_presence
    )
    GROUP BY stagiaire_id, id_action
),

union_stage AS (
    SELECT * FROM {{ ref("stg_stage_Soc_2") }}
    UNION ALL
    SELECT * FROM {{ ref("stg_stage_Soc_3") }}
),

heures_stage AS (
    SELECT
        stagiaire_id,
        IDAction,
        SUM(Duree) / 60.0 AS heures_stage
    FROM union_stage
    GROUP BY stagiaire_id, IDAction
),

heures_formateur AS (
    SELECT
        intervenant_id,
        id_action,
        SUM(TIME_DIFF(duree_seance, TIME(0, 0, 0), MINUTE)) / 60.0 AS heures_formateur_total
    FROM (
        SELECT DISTINCT intervenant_id, id_action, date_date, heure_debut, duree_seance
        FROM union_presence
        WHERE intervenant_id IS NOT NULL
    )
    GROUP BY intervenant_id, id_action
),

nb_inscrits_formateur AS (
    SELECT
        fr_formateur_id,
        IDAction,
        COUNT(*) AS nb_inscrits
    FROM dedup_inscrite
    WHERE fr_formateur_id IS NOT NULL
    GROUP BY fr_formateur_id, IDAction
),

-- Tarif principal par convention (est_principal = True, sinon premier tarif)
tarifs AS (
    SELECT
        id_convention,
        id_societe,
        nom_type_tarif,
        SAFE_CAST(tt_prix                       AS NUMERIC) AS tt_prix,
        SAFE_CAST(tt_prix_journee               AS NUMERIC) AS tt_prix_journee,
        SAFE_CAST(tt_prix_stagiaire_centre      AS NUMERIC) AS prix_stagiaire_centre,
        SAFE_CAST(tt_prix_stagiaire_entreprise  AS NUMERIC) AS prix_stagiaire_entrep,
        SAFE_CAST(tt_montant_heure              AS NUMERIC) AS tt_montant_heure,
        SAFE_CAST(tt_montant_centre             AS NUMERIC) AS tt_montant_centre,
        SAFE_CAST(tt_montant_entrep             AS NUMERIC) AS tt_montant_entrep,
        SAFE_CAST(tt_montant_global             AS NUMERIC) AS tt_montant_global,
        SAFE_CAST(tt_montant_journee            AS NUMERIC) AS tt_montant_journee,
        SAFE_CAST(tt_quantite_journee           AS NUMERIC) AS tt_quantite_journee
    FROM {{ ref('stg_tarif_convention') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_convention, id_societe
        ORDER BY CASE WHEN UPPER(est_principal) = 'TRUE' THEN 0 ELSE 1 END,
                 id_tarif_convention
    ) = 1
),

-- Taille du groupe par (convention × date d'entrée) — pour Forfait groupe / Montant global
groupe AS (
    SELECT
        conv_id,
        conv_id_societe,
        Date_Entree,
        COUNT(*) AS nb_inscrits_groupe
    FROM dedup_inscrite
    WHERE Date_Entree IS NOT NULL
    GROUP BY conv_id, conv_id_societe, Date_Entree
),

-- Jours ouvrés (lun-ven) entre Date_Entree et Date_Sortie_Previsionnelle par inscrit
jours_ouvres AS (
    SELECT
        stg_stagiaire_id,
        IDAction,
        (
            SELECT COUNT(*)
            FROM UNNEST(GENERATE_DATE_ARRAY(
                SAFE_CAST(Date_Entree              AS DATE),
                SAFE_CAST(Date_Sortie_Previsionnelle AS DATE)
            )) AS d
            WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)  -- 1=Dimanche, 7=Samedi
        ) AS nb_jours_ouvres
    FROM dedup_inscrite
    WHERE Date_Entree IS NOT NULL
      AND Date_Sortie_Previsionnelle IS NOT NULL
      AND SAFE_CAST(Date_Entree AS DATE) <= SAFE_CAST(Date_Sortie_Previsionnelle AS DATE)
),

-- Nb total d'inscrits par convention — pour ca_prevu_par_inscrit
nb_inscrits_conv AS (
    SELECT
        CAST(conv_id         AS STRING) AS conv_id,
        CAST(conv_id_societe AS STRING) AS conv_id_societe,
        COUNT(DISTINCT stg_stagiaire_id)  AS nb_inscrits
    FROM dedup_inscrite
    WHERE conv_id IS NOT NULL
    GROUP BY conv_id, conv_id_societe
),

-- Tiers stagiaire (examens) — pool global, même dataset pour toutes les sociétés
tiers AS (
    SELECT id_tiers, a_reussi_examen, a_passe_examen
    FROM {{ ref('stg_tiers_individuel_Soc_2') }}
),

-- Satisfaction (jointure sur email stagiaire — archive exclue car emails supprimés)
satisfaction AS (
    SELECT LOWER(TRIM(mail)) AS mail, note_globale AS note_satisfaction
    FROM {{ ref('stg_satisfaction_de') }}
    WHERE REGEXP_CONTAINS(mail, r'@')
),

-- Intervenants (para_sal_1 = est_formateur_externe)
intervenants AS (
    SELECT intervenant_id, '2' AS id_societe, est_formateur_externe FROM {{ ref('stg_intervenant_Soc_2') }}
    UNION ALL
    SELECT intervenant_id, '3' AS id_societe, est_formateur_externe FROM {{ ref('stg_intervenant_Soc_3') }}
    UNION ALL
    SELECT intervenant_id, '4' AS id_societe, est_formateur_externe FROM {{ ref('stg_intervenant_Soc_4') }}
),

-- Dimensions convention (groupe_ou_individuelle, dates) depuis les tables convention
conventions AS (
    SELECT
        CAST(IDConvent AS STRING)   AS id_convention,
        '2'                         AS id_societe,
        Groupe_Ou_Individuelle      AS groupe_ou_individuelle,
        Date_Debut                  AS date_debut,
        Date_Fin                    AS date_fin
    FROM {{ ref('stg_convention_Soc_2') }}
    UNION ALL
    SELECT
        CAST(IDConvent AS STRING)   AS id_convention,
        '4'                         AS id_societe,
        Groupe_Ou_Individuelle      AS groupe_ou_individuelle,
        Date_Debut                  AS date_debut,
        Date_Fin                    AS date_fin
    FROM {{ ref('stg_convention_Soc_4') }}
),

base AS (
    SELECT
        -- Identifiants parcours
        i.Code_Analytique_Parcours,
        i.Type_Region,
        i.Libelle_Court_Parcours,
        i.IDParcours_Groupe,
        i.Date_Entree,
        i.Date_Sortie,
        i.Date_Sortie_Previsionnelle,

        -- Formateur
        i.fr_formateur_id,
        CONCAT(COALESCE(i.fr_nom, ''), ' ', COALESCE(i.fr_prenom, '')) AS formateur_nom_complet,
        COALESCE(iv.est_formateur_externe, FALSE)                        AS est_formateur_externe,

        -- Examens
        COALESCE(tr.a_passe_examen,  FALSE)                              AS a_passe_examen,
        COALESCE(tr.a_reussi_examen, FALSE)                              AS a_reussi_examen,

        -- Satisfaction
        sat.note_satisfaction,

        -- Stagiaire
        i.stg_stagiaire_id,
        i.id_societe,
        i.stg_email_pro,
        i.stg_email_perso,

        -- Convention — identifiants (depuis inscrit)
        i.conv_id,
        i.conv_id_societe,
        i.conv_numero_financeur,
        i.conv_libelle2,
        i.conv_client_id,
        i.conv_client_nom,

        -- Convention — dimensions (depuis stg_convention)
        c.groupe_ou_individuelle,
        SAFE_CAST(c.date_debut AS DATE)     AS date_debut_convention,
        SAFE_CAST(c.date_fin   AS DATE)     AS date_fin_convention,
        DATE_DIFF(
            SAFE_CAST(c.date_fin   AS DATE),
            SAFE_CAST(c.date_debut AS DATE),
            DAY
        )                                   AS duree_convention_jours,

        -- BDC / Objectifs (depuis int_tarif_convention)
        bdc.montant_total_bdc,
        bdc.montant_centre_bdc,
        bdc.montant_entrep_bdc,
        bdc.duree_stagiaire_centre_bdc,
        bdc.duree_stagiaire_entrep_bdc,
        bdc.duree_prevue_heures_bdc,
        bdc.nb_stagiaire_prevu,
        nic.nb_inscrits,

        -- Tarif — type et prix unitaires
        t.nom_type_tarif,
        t.tt_prix,
        t.tt_prix_journee,
        t.tt_montant_journee,
        t.tt_quantite_journee,
        t.prix_stagiaire_centre,
        t.prix_stagiaire_entrep,
        g.nb_inscrits_groupe,

        -- Heures réalisées
        COALESCE(p.heures_realisees, 0)                              AS heures_realisees,
        COALESCE(p.heures_absence,   0)                              AS heures_absence,
        COALESCE(s.heures_stage,     0)                              AS heures_stage,
        SAFE_DIVIDE(f.heures_formateur_total, nif.nb_inscrits)       AS heures_formateur,

        -- Jours ouvrés (pour tarif à la journée)
        jo.nb_jours_ouvres,

        -- CA centre par inscrit selon le type de tarif
        CASE t.nom_type_tarif
            WHEN 'Heure par stagiaire' THEN
                COALESCE(p.heures_realisees, 0) * COALESCE(t.prix_stagiaire_centre, 0)
            WHEN 'Forfait stagiaire' THEN
                t.tt_prix
            WHEN 'Forfait groupe (ou forfait formateur)' THEN
                CASE
                    WHEN t.tt_prix_journee IS NOT NULL THEN
                        t.tt_prix_journee
                    ELSE
                        SAFE_DIVIDE(COALESCE(t.tt_montant_centre, t.tt_montant_heure), g.nb_inscrits_groupe)
                END
            WHEN 'Montant global' THEN
                SAFE_DIVIDE(t.tt_montant_global, g.nb_inscrits_groupe)
        END AS ca_centre,

        -- CA entreprise par inscrit selon le type de tarif
        CASE t.nom_type_tarif
            WHEN 'Heure par stagiaire' THEN
                COALESCE(s.heures_stage, 0) * COALESCE(t.prix_stagiaire_entrep, 0)
            WHEN 'Forfait stagiaire' THEN
                0
            WHEN 'Forfait groupe (ou forfait formateur)' THEN
                SAFE_DIVIDE(t.tt_montant_entrep, g.nb_inscrits_groupe)
            WHEN 'Montant global' THEN
                0
        END AS ca_entrep

    FROM dedup_inscrite i
    LEFT JOIN heures_presence      p   ON p.stagiaire_id    = i.stg_stagiaire_id
                                      AND p.id_action       = i.IDAction
    LEFT JOIN heures_stage         s   ON s.stagiaire_id    = i.stg_stagiaire_id
                                      AND s.IDAction        = i.IDAction
    LEFT JOIN heures_formateur     f   ON f.intervenant_id  = i.fr_formateur_id
                                      AND f.id_action       = i.IDAction
    LEFT JOIN nb_inscrits_formateur nif ON nif.fr_formateur_id = i.fr_formateur_id
                                       AND nif.IDAction        = i.IDAction
    LEFT JOIN tarifs               t   ON t.id_convention   = CAST(i.conv_id        AS STRING)
                                      AND t.id_societe      = CAST(i.conv_id_societe AS STRING)
    LEFT JOIN groupe               g   ON g.conv_id         = i.conv_id
                                      AND g.conv_id_societe = i.conv_id_societe
                                      AND g.Date_Entree     = i.Date_Entree
    LEFT JOIN jours_ouvres         jo  ON jo.stg_stagiaire_id = i.stg_stagiaire_id
                                      AND jo.IDAction         = i.IDAction
    LEFT JOIN conventions          c   ON c.id_convention   = CAST(i.conv_id        AS STRING)
                                      AND c.id_societe      = CAST(i.conv_id_societe AS STRING)
    LEFT JOIN {{ ref('int_tarif_convention') }} bdc
                                      ON bdc.id_convention  = CAST(i.conv_id        AS STRING)
                                      AND bdc.id_societe    = CAST(i.conv_id_societe AS STRING)
    LEFT JOIN nb_inscrits_conv     nic ON nic.conv_id        = CAST(i.conv_id        AS STRING)
                                      AND nic.conv_id_societe = CAST(i.conv_id_societe AS STRING)
    LEFT JOIN intervenants         iv  ON CAST(iv.intervenant_id AS STRING) = CAST(i.fr_formateur_id AS STRING)
                                      AND iv.id_societe      = i.id_societe
    LEFT JOIN tiers                tr  ON tr.id_tiers = CAST(i.stg_stagiaire_id AS STRING)
    LEFT JOIN satisfaction         sat ON sat.mail = LOWER(TRIM(i.stg_email_pro))
                                      OR sat.mail = LOWER(TRIM(i.stg_email_perso))
)

SELECT
    *,
    COALESCE(ca_centre, 0) + COALESCE(ca_entrep, 0) AS ca_total
FROM base
