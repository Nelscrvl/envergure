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

classification as (

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
        WHEN statut NOT IN ("Demande sans suite","Annulé") THEN 1
        ELSE NULL
    END AS oriente,

CASE WHEN statut IN ("Prestation aboutie") THEN 1
        ELSE NULL
    END AS prime,
CASE WHEN statut = "Sortie anticipée" AND situation NOT LIKE "Reprise d'emploi de moins de 6 mois" AND situation LIKE "Reprise%" OR situation LIKE "Retour%" THEN 1 
        ELSE NULL
    END AS sortie_anticipee_rep_emploi,

CASE WHEN  statut = "Prestation aboutie"
     AND (
            situation LIKE "Reprise%"
         OR situation LIKE "Retour%"
         )
THEN 1
    ELSE NULL
    END AS Prestation_aboutie_rep_emploi,
CASE WHEN statut = "Prestation aboutie"
     AND NOT (
         situation = "Reprise d'emploi de moins de 6 mois"
         OR situation = "Retour à l'emploi"
         OR situation = "Emploi durable"
         OR situation = "Reprise d'emploi CDI ou CDD/CTT + de 6 mois + de 78h/mois"
     )
THEN 1
    ELSE NULL
    END AS Prestation_aboutie,
CASE WHEN (statut = "Prestation aboutie" 
      OR statut = "Sortie anticipée")
     AND situation = "Reprise d'emploi CDI ou CDD/CTT + de 6 mois + de 78h/mois"
THEN 1
    ELSE NULL
    END AS retour_emploi,

CASE WHEN date_fin_prest IS NULL THEN 1
    ELSE 0
    END AS portefeuille,
CASE 
    WHEN statut = "Sortie anticipée" THEN 1
    ELSE 0
END AS sortie_anticipee,
CASE 
    WHEN statut ="Sortie anticipée" AND NOT situation ="Reprise d'emploi CDI ou CDD/CTT + de 6 mois + de 78h/mois" THEN 1
    ELSE 0
END AS sorties_anticipees_hors_sorties_positives,
CASE WHEN statut NOT IN ("Demande sans suite","Annulé","Ne s'est pas présenté") THEN 1
    ELSE 0
    END AS recu,
CAST(FORMAT_DATE('%Y%m', date_demarrage) AS INT64) AS annee_mois_sort

FROM prep_data 

),
prime_unique AS (
  SELECT *
  FROM (
    SELECT 
      b.*,
      ROW_NUMBER() OVER (
        PARTITION BY id_benef 
        ORDER BY Date_primes ASC
      ) as rn
    FROM {{ref("int_referrent_rdv_ca")}} b
    WHERE Montant_prime > 1
  )
  WHERE rn = 1
)

SELECT 
  c.*,
  CASE 
    WHEN retour_emploi = 1 OR p.Date_primes >= "2021-01-01" THEN 1
    ELSE 0
  END AS retour_emploi_env_semap,
  p.Montant_prime,
  p.Date_primes
FROM classification c
LEFT JOIN prime_unique p
  ON c.id_benef = p.id_benef
