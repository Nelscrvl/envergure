WITH jointure AS (
    SELECT
rdv.instance,
rdv.id_benef,
id_rdv,
date_deb,
date_fin,
type,
typage,
objet,
statut,
rdv.referent,
rdv.id_referent,
rdv.site,
rdv.id_site,
situation,
suivi,
CASE 
    WHEN present = 1 THEN "present" 
    WHEN present = 0 THEN "absent"
    WHEN present = 2 THEN "annulé"
    WHEN present IS NULL THEN "a catégoriser"
ELSE "" 
END AS presence,
rdv.marche,
rdv.lot as lot,
trim(substr(rdv.lot, strpos(rdv.lot, ' ') + 1)) as lot_sans_action,
benef.organisme,
precision_rdv

FROM {{ref("stg_rdv")}} as rdv
LEFT JOIN {{ref("stg_benef")}} as benef
ON rdv.id_benef = benef.id_benef
-- id_benef n'est unique qu'au sein d'une instance SI-Emploi
AND rdv.instance = benef.instance

),

granularite AS (
SELECT 
*,
CASE 
    WHEN presence = "present"  THEN TIMESTAMP_DIFF(
             TIMESTAMP(date_fin),
             TIMESTAMP(date_deb),
             MINUTE
           )
    ELSE 0
END AS tps_travail_ref
FROM jointure
)


SELECT
COUNT(id_benef) AS nb_de_personne_presente,
instance,
id_benef,
date_deb,
date_fin,
type,
typage,
referent,
id_referent,
site,
id_site,
presence,
lot_sans_action,
marche,
lot,
organisme,
tps_travail_ref,
statut,
situation,
precision_rdv,
suivi
FROM granularite
GROUP BY date_deb,
instance,
id_benef,
date_fin,
type,
typage,
referent,
id_referent,
site,
id_site,
presence,
marche,
lot,
lot_sans_action,
organisme,
tps_travail_ref,
statut,
situation,
precision_rdv,
suivi