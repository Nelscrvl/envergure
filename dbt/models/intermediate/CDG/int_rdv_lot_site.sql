WITH jointure AS (
    SELECT 
rdv.id_benef,
id_rdv,
date_deb,
date_fin,
type,
typage,
objet,
rdv.referent,
rdv.id_referent,
rdv.site,
rdv.id_site,
CASE 
    WHEN present = 1 THEN "present" 
    WHEN present = 0 THEN "absent"
    WHEN present = 2 THEN "annulé"
    WHEN present IS NULL THEN "a catégoriser"
ELSE "" 
END AS presence,
split(lot, ' ')[offset(0)] as marche,
lot,
trim(substr(lot, strpos(lot, ' ') + 1)) as lot_sans_action,
organisme

FROM {{ref("stg_rdv")}} as rdv
LEFT JOIN {{ref("stg_benef")}} as benef
ON rdv.id_benef = benef.id_benef

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
tps_travail_ref
FROM granularite
GROUP BY date_deb,
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
tps_travail_ref
