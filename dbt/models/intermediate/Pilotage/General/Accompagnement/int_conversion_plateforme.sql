-- Parcours des prestations proposées à France Travail, mois par mois.
--
-- D'après Sirom : une planification envoyée à FT n'est connue que si elle a été posée
-- dans SI-Emploi en plage réservée (RDV de type "Réservé plateforme"). Tant que FT ne
-- l'a pas convertie en bon de commande, elle reste un créneau ; une fois convertie, elle
-- devient un dossier (avec sa lettre de commande). Un créneau dont la date est passée
-- n'a donc pas été converti ; un créneau à venir est encore ouvert.
--
-- Les deux flux (créneaux, dossiers) sont empilés puis agrégés sur les mêmes dimensions,
-- pour que Looker n'ait pas à les combiner. Les taux se calculent dans Looker sur les sommes.

WITH creneaux AS (

    SELECT
        DATE_TRUNC(DATE(LEFT(date_deb, 10)), MONTH)     AS mois,
        DATE(LEFT(date_deb, 10)) < CURRENT_DATE()       AS est_passe,
        instance,
        organisme,
        NULLIF(marche, '')                              AS marche,
        lot,
        site,
        referent
    FROM {{ ref('stg_rdv') }}
    WHERE type = 'Réservé plateforme'

),

-- Région et numéro de lot des créneaux : repris des dossiers du même lot, pour suivre
-- exactement les règles de int_pilotage_global (les créneaux n'ont pas de code postal).
lots AS (

    SELECT
        lot,
        APPROX_TOP_COUNT(region, 1)[SAFE_OFFSET(0)].value AS region,
        ANY_VALUE(lot_unique)                              AS lot_unique
    FROM {{ ref('int_pilotage_global') }}
    WHERE lot IS NOT NULL AND region IS NOT NULL
    GROUP BY lot

),

dossiers AS (

    SELECT
        DATE_TRUNC(date_demarrage, MONTH)               AS mois,
        instance,
        organisme,
        NULLIF(marche, '')                              AS marche,
        region,
        lot,
        lot_unique,
        site,
        referent,
        -- prestation démarrée : mêmes statuts que adhesion = 1 dans int_pilotage_global
        statut IN ('Prestation aboutie', 'Prestation en cours', 'Sortie anticipée') AS est_realise
    FROM {{ ref('int_pilotage_global') }}
    WHERE date_demarrage IS NOT NULL

),

flux AS (

    SELECT
        c.mois, c.instance, c.organisme, c.marche, l.region, c.lot, l.lot_unique, c.site, c.referent,
        IF(c.est_passe, 1, 0)       AS creneaux_non_convertis,
        IF(c.est_passe, 0, 1)       AS creneaux_ouverts,
        0                           AS dossiers_commandes,
        0                           AS dossiers_realises
    FROM creneaux AS c
    LEFT JOIN lots AS l USING (lot)

    UNION ALL

    SELECT
        mois, instance, organisme, marche, region, lot, lot_unique, site, referent,
        0, 0, 1, IF(est_realise, 1, 0)
    FROM dossiers

),

agg AS (

    SELECT
        mois, instance, organisme, marche, region, lot, lot_unique, site, referent,
        SUM(creneaux_non_convertis)   AS creneaux_non_convertis,
        SUM(creneaux_ouverts)         AS creneaux_ouverts,
        SUM(dossiers_commandes)       AS dossiers_commandes,
        SUM(dossiers_realises)        AS dossiers_realises
    FROM flux
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

),

-- Un marché n'est planifié en plages réservées que pour certains organismes (ATR, VS2,
-- SPR... ne le sont pas chez Envergure). Seuil de 1 % de créneaux dans le flux de
-- l'organisme, pour ne pas qualifier un marché sur quelques créneaux isolés.
marches_planifies AS (

    SELECT organisme, marche
    FROM agg
    WHERE marche IS NOT NULL AND organisme IS NOT NULL
    GROUP BY 1, 2
    HAVING SAFE_DIVIDE(
        SUM(creneaux_non_convertis + creneaux_ouverts),
        SUM(creneaux_non_convertis + creneaux_ouverts + dossiers_commandes)
    ) >= 0.01

)

SELECT
    a.mois,
    a.instance,
    a.organisme,
    a.marche,
    mp.marche IS NOT NULL           AS marche_planifie_si_emploi,
    a.region,
    a.lot,
    a.lot_unique,
    a.site,
    a.referent,
    a.creneaux_non_convertis,
    a.creneaux_ouverts,
    a.dossiers_commandes,
    a.dossiers_realises
FROM agg AS a
LEFT JOIN marches_planifies AS mp
    ON mp.organisme = a.organisme AND mp.marche = a.marche
