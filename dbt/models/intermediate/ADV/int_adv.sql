WITH prep_data AS ( 
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
split(lot, ' ')[offset(0)] as marche,
lot,
trim(substr(lot, strpos(lot, ' ') + 1)) as lot_sans_action,
site,
id_site,
organisme,
statut,
situation,
util_exclu_factu,
date_exclu_factu,
CASE 
    WHEN statut IN("Demande sans suite","Ne s'est pas présenté") THEN 0
    ELSE 1
END AS activite,
FROM {{ref("stg_benef")}}

),

catego AS (

SELECT 
    *,
    CASE 
        WHEN activite = 1 AND statut = "N'a pas adhéré" THEN 0
        WHEN activite = 1 AND statut IN ("Prestation aboutie", "Sortie anticipée", "Prestation en cours") THEN 1
        ELSE NULL
    END AS adhesion,
    CASE 
        WHEN activite = 1 AND statut = "N'a pas adhéré" THEN 1
        WHEN activite = 1 AND statut IN ("Prestation aboutie", "Sortie anticipée", "Prestation en cours") THEN 0
        ELSE NULL
    END AS non_adhere,
FROM prep_data
)

SELECT 
stg_facture.*,
catego.*EXCEPT(id_benef),
date_diff(date_facture,date_fin_prest, DAY) AS nb_jour_fin_presta_paiment
FROM {{ref("stg_facture")}}
FULL JOIN catego
on catego.id_benef = stg_facture.id_benef