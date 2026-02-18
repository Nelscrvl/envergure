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
CASE 
    WHEN statut IN("Demande sans suite","Ne s'est pas présenté") THEN 0
    ELSE 1
END AS activite
FROM {{ref("stg_benef")}}

)

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
CASE 
        WHEN statut IN ("Prestation aboutie", "Sortie anticipée", "Prestation en cours","convoqué","Demande sans suite","ne s'est pas présenté","convoqué") THEN 1
        ELSE NULL
    END AS oriente,

CASE WHEN statut IN ("Prestation aboutie", "Sortie anticipée", "Prestation en cours","convoqué","Demande sans suite") THEN 1
        ELSE NULL
    END AS recu,
CASE WHEN statut IN ("Prestation aboutie") THEN 1
        ELSE NULL
    END AS prime

FROM prep_data
