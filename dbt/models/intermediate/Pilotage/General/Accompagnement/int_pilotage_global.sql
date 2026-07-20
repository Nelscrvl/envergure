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
id_presta_coll,
lot,
cp_benef,
-- cp_benef perd parfois son zéro initial (ex. "3200" pour "03200") d'où le LPAD avant lecture du département
LPAD(TRIM(cp_benef), 5, '0') AS cp_benef_norm,
-- underscore -> espace pour que les \b des regex région matchent aussi "CVL_Lot", "HN_LOT", etc.
REGEXP_REPLACE(lot, r'_', ' ') AS lot_norm,
COALESCE(
  -- paire de départements en fin de lot ("...DD/DD")
  REGEXP_EXTRACT(lot, r'(\d{2})/\d{2}\s*$'),
  -- département unique après le dernier underscore, convention PPAIP_LOT n_DD uniquement
  CASE WHEN REGEXP_CONTAINS(lot, r'(?i)^PPAIP') THEN REGEXP_EXTRACT(lot, r'_(\d{2})\s*$') END,
  -- sinon département du bénéficiaire
  SUBSTR(LPAD(TRIM(cp_benef), 5, '0'), 1, 2)
) AS dept_code,
CASE
  WHEN split(lot, ' ')[safe_offset(0)] = ''
  THEN SUBSTR(numero_commande, 3, 3)
  ELSE split(lot, ' ')[safe_offset(0)]
END AS marche,
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

region_data AS (

SELECT
    *,
    CASE
      -- Outre-mer (noms uniques, pas d'ambiguïté)
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bMAYOTTE\b')                              THEN 'MAYOTTE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bGUADELOUPE\b|\bGUAD\b')                 THEN 'GUADELOUPE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bMARTINIQUE\b')                           THEN 'MARTINIQUE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bRÉUNION\b|\bREUNION\b|\bRN\b')          THEN 'REUNION'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bRM\b')                                   THEN 'REUNION-MARTINIQUE'  -- à confirmer avec métier

      -- Patterns multi-mots en premier (avant les abbréviations courtes)
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bGRAND[\s_-]?EST\b|\bCHAMPAGNE\b|\bLORRAINE\b|\bALSACE\b|\bMARNE\b') THEN 'GRAND EST'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bHAUTS[\s-]DE[\s-]FRANCE\b')             THEN 'HAUTS-DE-FRANCE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bNOUVELLE[\s-]AQUITAINE\b')              THEN 'NOUVELLE-AQUITAINE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bCENTRE[\s-]VAL\b|\bEURE ET LOIR\b')    THEN 'CENTRE-VAL DE LOIRE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bBOURGOGNE\b|\bFRANCHE[\s-]COMT')       THEN 'BOURGOGNE-FRANCHE-COMTE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bSEINE[\s-]MARITIME\b|\bNORMANDIE\b')   THEN 'NORMANDIE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bAUVERGNE\b')                            THEN 'AUVERGNE-RHONE-ALPES'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bPAYS[\s-]DE[\s-]LA[\s-]LOIRE\b')       THEN 'PAYS DE LA LOIRE'

      -- Abréviations courtes (word boundary \b essentiel ; lot_norm remplace les underscores par des
      -- espaces pour que \b fonctionne aussi sur "CVL_Lot", "HN_LOT", "BG_Lot", "_HDF", etc.)
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bGDEST\b|\bGE\b')                        THEN 'GRAND EST'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bBFC\b|\bFC\b|\bBG\b')                   THEN 'BOURGOGNE-FRANCHE-COMTE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bNOR\b|\bNORM\b|\bHN\b')                 THEN 'NORMANDIE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bBRETAGNE\b')                            THEN 'BRETAGNE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bHDF\b')                                  THEN 'HAUTS-DE-FRANCE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bIDF\b')                                  THEN 'ILE-DE-FRANCE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bPACA\b')                                 THEN 'PACA'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bPDL\b')                                  THEN 'PAYS DE LA LOIRE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bNAQ\b')                                  THEN 'NOUVELLE-AQUITAINE'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bAURA\b|\bARA\b')                        THEN 'AUVERGNE-RHONE-ALPES'
      WHEN REGEXP_CONTAINS(lot_norm, r'(?i)\bCVL\b|\bCENTRE\b')                      THEN 'CENTRE-VAL DE LOIRE'

      -- Fallback département (01-95 + Corse) : dept_code priorise le département déduit du lot (paire
      -- "DD/DD" en fin de chaîne, ou convention PPAIP_LOT n_DD), sinon celui du bénéficiaire (cp_benef)
      WHEN REGEXP_CONTAINS(dept_code, r'^(04|05|06|13|83|84)')                        THEN 'PACA'
      WHEN REGEXP_CONTAINS(dept_code, r'^(01|03|07|15|26|38|42|43|63|69|73|74)')      THEN 'AUVERGNE-RHONE-ALPES'
      WHEN REGEXP_CONTAINS(dept_code, r'^(02|59|60|62|80)')                           THEN 'HAUTS-DE-FRANCE'
      WHEN REGEXP_CONTAINS(dept_code, r'^(08|10|51|52|54|55|57|67|68|88)')            THEN 'GRAND EST'
      WHEN REGEXP_CONTAINS(dept_code, r'^(09|11|12|30|31|32|34|46|48|65|66|81|82)')   THEN 'OCCITANIE'
      WHEN REGEXP_CONTAINS(dept_code, r'^(14|27|50|61|76)')                           THEN 'NORMANDIE'
      WHEN REGEXP_CONTAINS(dept_code, r'^(21|25|39|58|70|71|89|90)')                  THEN 'BOURGOGNE-FRANCHE-COMTE'
      WHEN REGEXP_CONTAINS(dept_code, r'^(22|29|35|56)')                              THEN 'BRETAGNE'
      WHEN REGEXP_CONTAINS(dept_code, r'^(16|17|19|23|24|33|40|47|64|79|86|87)')      THEN 'NOUVELLE-AQUITAINE'
      WHEN REGEXP_CONTAINS(dept_code, r'^(18|28|36|37|41|45)')                        THEN 'CENTRE-VAL DE LOIRE'
      WHEN REGEXP_CONTAINS(dept_code, r'^(44|49|53|72|85)')                           THEN 'PAYS DE LA LOIRE'
      WHEN REGEXP_CONTAINS(dept_code, r'^(75|77|78|91|92|93|94|95)')                  THEN 'ILE-DE-FRANCE'
      WHEN REGEXP_CONTAINS(dept_code, r'^(20)')                                       THEN 'CORSE'

      -- Outre-mer via cp_benef (préfixe 3 chiffres — dept_code ne garde que 2 chiffres, insuffisant ici)
      WHEN REGEXP_CONTAINS(cp_benef_norm, r'^971')                                    THEN 'GUADELOUPE'
      WHEN REGEXP_CONTAINS(cp_benef_norm, r'^972')                                    THEN 'MARTINIQUE'
      WHEN REGEXP_CONTAINS(cp_benef_norm, r'^973')                                    THEN 'GUYANE'
      WHEN REGEXP_CONTAINS(cp_benef_norm, r'^974')                                    THEN 'REUNION'
      WHEN REGEXP_CONTAINS(cp_benef_norm, r'^976')                                    THEN 'MAYOTTE'

      ELSE NULL  -- cas non résolu (lot et cp_benef vides ou non exploitables)
    END AS region
FROM prep_data

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

FROM region_data

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
CASE WHEN ap.marche IN("ATR","ESPR","VS2","SPR") AND rnk_lc =1 THEN 1 
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
