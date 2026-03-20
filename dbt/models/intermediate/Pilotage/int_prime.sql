WITH rename AS (
    
    SELECT

`Numéro du lot` AS lot_unique,

CASE 
    WHEN `intitulé du lot` LIKE "%CVL%" 
      OR `intitulé du lot` LIKE "%IDF%" 
      OR `intitulé du lot` LIKE "%GUAD%" 
    THEN SPLIT(`intitulé du lot`, ' ')[SAFE_OFFSET(2)] 
    WHEN `intitulé du lot` LIKE "%GDEST%" THEN "GE"
END AS region,

`Numéro de marché` AS numero_marche,
`Date de debut` AS date_deb,
`Date de fin` AS date_fin,
`Raison sociale titulaire` AS organisme,
`identifiant local` AS id_local,
`Resultat déclaré` AS resultat_declare,
`Statut de la prestation` AS statut_presta, 
`Libelle du statut de la prestation` AS lib_statut_presta,
`Date de dernier statut` AS date_der_statut,
DPAE,
DSN,
`Situation DE` AS situation_DE,
`Montant facturé` AS montant_facture,
`Réfaction` AS refaction,
`Certificat émis` AS certificat_emis,
`Date détection prime` AS date_detection_prime,
`Date détection complément` AS date_detection_complement,
`Numero de commande`AS numero_de_commande,
LC

FROM {{ref("int_union_all_prime")}}


)

SELECT
r.*,
b.id_benef
FROM rename as r 
LEFT JOIN {{ref("stg_benef")}} as b
on r.LC = b.numero_commande