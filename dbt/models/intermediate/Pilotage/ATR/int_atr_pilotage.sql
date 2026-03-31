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

)

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
EXTRACT(MONTH FROM CAST(date_deb AS TIMESTAMP)) AS mois_num
FROM KPI
ORDER BY date_deb ASC