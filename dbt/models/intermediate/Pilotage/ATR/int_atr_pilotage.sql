WITH  prep_data AS (

SELECT
id_benef,
nom,
ide,
agence,
conseiller,
referent,
id_ref_principal,
date_demarrage,
date_fin_theo,
date_fin_prest,
numero_commande,
split(lot, ' ')[safe_offset(0)] as marche,
split(lot, ' ')[safe_offset(1)] as region,
split(lot, ' ')[safe_offset(2)] as lot_unique,
trim(substr(lot, strpos(lot, ' ') + 1)) as lot_sans_action,
site,
id_site,
organisme,
statut,
situation,
id_presta_coll,
CASE 
    WHEN statut IN("Demande sans suite","Ne s'est pas présenté") THEN 0
    ELSE 1
END AS activite
FROM {{ref("stg_benef")}}
),


jointure AS 

(SELECT 
pd.*,
r.date_deb,
r.date_fin,
r.type,
r.objet,
r.present,
r.motif_absence,
r.theme,
r.module,
r.commentaire,
TIMESTAMP_DIFF(CAST(r.date_fin AS TIMESTAMP),CAST(r.date_deb AS TIMESTAMP),HOUR) AS duree_seance
FROM prep_data as pd 
LEFT JOIN {{ref('stg_rdv')}} as r
ON pd.id_benef = r.id_benef
WHERE marche ="ATR"

),

KPI AS (SELECT 
*,
CASE WHEN statut NOT IN ("Annulé") THEN 1 
    ELSE 0
    END AS oriente,
CASE WHEN statut IN ("n'a pas adhéré","Prestation aboutie","Prestation en cours","Sortie anticipée") THEN 1
    ELSE 0
END AS recu,
    CASE 
        WHEN activite = 1 AND statut IN ("Prestation aboutie", "Sortie anticipée", "Prestation en cours") THEN 1
        ELSE 0
    END AS adhesion,

FROM jointure

),

creation_rang AS (
    SELECT
*,
CASE
    WHEN duree_seance > 3 AND recu = 1  THEN "longue"
    WHEN duree_seance <=3 AND recu = 1 THEN "courte"
    ELSE ""
END AS type_atelier,
CASE 
    WHEN statut = "Annulé" THEN 1
    ELSE 0
END AS session_annulee,
CASE 
    WHEN statut = "Annulé" THEN "Annulé"
    ELSE "Réalisé"
END AS type_session,
EXTRACT(MONTH FROM CAST(date_deb AS TIMESTAMP)) AS mois_num,
ROW_NUMBER() OVER (
            PARTITION BY numero_commande
            ORDER BY date_deb ASC
        ) AS rang_entree,
FROM KPI

),

facture_dedup AS (
    SELECT id_presta_coll, MAX(date_facture) AS date_facture, MAX(montant_HT) AS montant_HT
    FROM {{ref('stg_facture')}}
    GROUP BY id_presta_coll
),

jointure_facture AS (
    SELECT
    c.*,
    f.date_facture,
    CASE
        WHEN rang_entree = 1 THEN f.montant_HT
        ELSE 0
    END AS ca_atelier
    FROM creation_rang AS c
    LEFT JOIN facture_dedup AS f
    ON c.id_presta_coll = f.id_presta_coll
)

--final AS (

SELECT
*,
MAX(ca_atelier) OVER (PARTITION BY numero_commande) / NULLIF(MAX(rang_entree) OVER (PARTITION BY numero_commande), 0) AS ca_atelier_par_benef,
CASE
    WHEN numero_commande <> ""
    AND rang_entree = MIN(CASE WHEN type_atelier IN ("longue","courte") OR ca_atelier > 0 THEN rang_entree ELSE NULL END) OVER (PARTITION BY numero_commande)
    AND (type_atelier IN ("longue","courte") OR ca_atelier > 0) THEN 1
    ELSE 0
END AS nb_atelier_real,
CASE
    WHEN date_demarrage <= CURRENT_DATE -2 AND statut ="Convoqué" THEN 1
    ELSE 0
END AS statut_pas_a_jour
FROM jointure_facture


-- TEST 1 : comparaison des counts entre creation_rang et jointure_facture
-- SELECT 'creation_rang' AS source, COUNT(*) AS nb FROM creation_rang
-- UNION ALL
-- SELECT 'jointure_facture' AS source, COUNT(*) AS nb FROM jointure_facture

-- TEST 2 : résultat final
--SELECT

--COUNT(DISTINCT numero_commande)
--SUM(ca_atelier)
--SUM(nb_atelier_real)
--FROM final
--WHERE organisme = "ENVERGURE" AND FORMAT_TIMESTAMP('%Y-%m', CAST(date_demarrage AS TIMESTAMP)) = "2026-03"  AND marche ="ATR"
