WITH prep_data AS ( 
    SELECT
instance,    
id_benef,
numero_commande,
nom,
email,
ide,
agence,
conseiller,
referent,
id_ref_principal,
date_demarrage,
date_fin_theo,
date_fin_prest,
lot,
CASE
  WHEN split(lot, ' ')[safe_offset(0)] = ''
  THEN SUBSTR(numero_commande, 3, 3)
  ELSE split(lot, ' ')[safe_offset(0)]
END AS marche,
CASE
  -- Outre-mer (noms uniques, pas d'ambiguïté)
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bMAYOTTE\b')                              THEN 'MAYOTTE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bGUADELOUPE\b|\bGUAD\b')                 THEN 'GUADELOUPE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bMARTINIQUE\b')                           THEN 'MARTINIQUE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bRÉUNION\b|\bREUNION\b|\bRN\b')          THEN 'REUNION'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bRM\b')                                   THEN 'REUNION-MARTINIQUE'  -- à confirmer avec métier

  -- Patterns multi-mots en premier (avant les abbréviations courtes)
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bGRAND[\s_-]?EST\b|\bCHAMPAGNE\b|\bLORRAINE\b|\bALSACE\b') THEN 'GRAND EST'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bHAUTS[\s-]DE[\s-]FRANCE\b')             THEN 'HAUTS-DE-FRANCE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bNOUVELLE[\s-]AQUITAINE\b')              THEN 'NOUVELLE-AQUITAINE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bCENTRE[\s-]VAL\b|\bEURE ET LOIR\b')    THEN 'CENTRE-VAL DE LOIRE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bBOURGOGNE\b|\bFRANCHE[\s-]COMT')       THEN 'BOURGOGNE-FRANCHE-COMTE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bSEINE[\s-]MARITIME\b|\bNORMANDIE\b')   THEN 'NORMANDIE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bAUVERGNE\b')                            THEN 'AUVERGNE-RHONE-ALPES'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bPAYS[\s-]DE[\s-]LA[\s-]LOIRE\b')       THEN 'PAYS DE LA LOIRE'

  -- Abréviations courtes (word boundary \b essentiel)
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bGDEST\b|\bGE\b')                        THEN 'GRAND EST'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bBFC\b|\bFC\b|\bBG\b')                   THEN 'BOURGOGNE-FRANCHE-COMTE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bNOR\b|\bNORM\b|\bHN\b')                 THEN 'NORMANDIE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bBRETAGNE\b')                            THEN 'BRETAGNE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bHDF\b')                                  THEN 'HAUTS-DE-FRANCE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bIDF\b')                                  THEN 'ILE-DE-FRANCE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bPACA\b')                                 THEN 'PACA'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bPDL\b')                                  THEN 'PAYS DE LA LOIRE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bNAQ\b')                                  THEN 'NOUVELLE-AQUITAINE'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bAURA\b|\bARA\b')                        THEN 'AUVERGNE-RHONE-ALPES'
  WHEN REGEXP_CONTAINS(lot, r'(?i)\bCVL\b|\bCENTRE\b')                      THEN 'CENTRE-VAL DE LOIRE'

  ELSE NULL  -- cas non résolu
END AS region
,
COALESCE(
  -- P1 : mot-clé LOT explicite (LOT 3, LOT_3, LOT3, LOT100, _Lot 3_, etc.)
  REGEXP_EXTRACT(lot, r'(?i)(?:^|[\s_])LOT[_\s]*(\d{1,3})(?:[_\s]|$)'),
  -- P2 : [LETTRES] [1-2 chiffres] - [5 chiffres] en fin de chaîne (DSE, UES, SPR)
  REGEXP_EXTRACT(lot, r'[A-Za-z]+\s+(\d{1,2})\s+-\s+\d{5}$'),
  -- P3 : 3ème token si 1-2 chiffres (AGIR FAMI 12, ATR GE 2 avec parenthèses, …)
  REGEXP_EXTRACT(lot, r'^\S+\s+\S+\s+(\d{1,2})(?:\s|$)')
) AS lot_unique,
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

ajout_prime AS (
SELECT 
  c.* EXCEPT(prime),
  CASE 
    WHEN retour_emploi = 1 OR p.Date_primes >= "2021-01-01" THEN 1
    ELSE 0
  END AS retour_emploi_env_semap,
  p.Montant_prime,
  p.Date_primes,
  CASE WHEN p.Date_primes >= "2021-01-01" THEN 1
  ELSE 0
  END AS prime,
  ROW_NUMBER() OVER (PARTITION BY numero_commande ORDER BY date_demarrage ASC) AS rnk_lc

FROM classification c
LEFT JOIN {{(ref("stg_prime_compile"))}} p
  ON c.numero_commande = p.LC

),

ajout_note AS (
SELECT
ap.*,
CASE WHEN ap.marche IN("ATR","VS2","ESPR") AND rnk_lc =1 THEN 1 
ELSE 0
END AS dont_nb_sessions,
CASE WHEN REGEXP_CONTAINS(CAST(sd.note_globale AS STRING), r'^\d+$') THEN CAST(sd.note_globale AS INT64) ELSE NULL END AS note_globale
FROM ajout_prime AS ap
LEFT JOIN {{(ref('stg_satisfaction_de'))}} AS sd
ON ap.email = sd.mail
),

final AS (
SELECT
an.*,
CASE WHEN note_globale >=0 AND note_globale <= 5 THEN 1 
ELSE 0 
END AS nb_de_note,
CASE WHEN rnk_lc =1 AND statut = "Annulé" THEN 1 
ELSE 0
END AS dont_sessions_annulee,
p.total_nb_heure,
p.nb_pmsmp
FROM ajout_note as an
LEFT JOIN {{(ref("int_pmsmp"))}} as p
ON an.id_benef = p.id_benef
)
SELECT
*
FROM final

