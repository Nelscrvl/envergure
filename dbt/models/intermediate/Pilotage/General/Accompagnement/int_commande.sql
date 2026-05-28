SELECT
  instance,
  id_benef,
  CAST(NULL AS STRING) AS numero_commande,
  CAST(NULL AS STRING) AS nom,
  CAST(NULL AS STRING) AS email,
  CAST(NULL AS STRING) AS ide,
  CAST(NULL AS STRING) AS agence,
  CAST(NULL AS STRING) AS conseiller,
  referent,
  CAST(NULL AS INT64) AS id_ref_principal,
  PARSE_DATE('%Y-%m-%d', LEFT(date_deb, 10)) AS date_demarrage,
  CAST(NULL AS DATE) AS date_fin_theo,
  PARSE_DATE('%Y-%m-%d', LEFT(date_fin, 10)) AS date_fin_prest,
  CAST(NULL AS INT64) AS id_presta_coll,
  lot,
  split(lot, ' ')[safe_offset(0)] AS marche,
  CASE
    -- Outre-mer (noms uniques, pas d'ambiguïté)
    WHEN REGEXP_CONTAINS(lot, r'(?i)\bMAYOTTE\b')                              THEN 'MAYOTTE'
    WHEN REGEXP_CONTAINS(lot, r'(?i)\bGUADELOUPE\b|\bGUAD\b')                 THEN 'GUADELOUPE'
    WHEN REGEXP_CONTAINS(lot, r'(?i)\bMARTINIQUE\b')                           THEN 'MARTINIQUE'
    WHEN REGEXP_CONTAINS(lot, r'(?i)\bRÉUNION\b|\bREUNION\b|\bRN\b')          THEN 'REUNION'
    WHEN REGEXP_CONTAINS(lot, r'(?i)\bRM\b')                                   THEN 'REUNION-MARTINIQUE'

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
    ELSE NULL
  END AS region,
  COALESCE(
    REGEXP_EXTRACT(lot, r'(?i)(?:^|[\s_])LOT[_\s]*(\d{1,3})(?:[_\s]|$)'),
    REGEXP_EXTRACT(lot, r'[A-Za-z]+\s+(\d{1,2})\s+-\s+\d{5}$'),
    REGEXP_EXTRACT(lot, r'^\S+\s+\S+\s+(\d{1,2})(?:\s|$)')
  ) AS lot_unique,
  TRIM(SUBSTR(lot, STRPOS(lot, ' ') + 1)) AS lot_sans_action,
  site,
  id_site,
  CAST(NULL AS STRING) AS organisme,
  type AS statut,
  CAST(NULL AS STRING) AS situation,
  NULL AS activite,
  NULL AS adhesion,
  NULL AS non_adhere,
  NULL AS oriente,
  NULL AS sortie_anticipee_rep_emploi,
  NULL AS Prestation_aboutie_rep_emploi,
  NULL AS Prestation_aboutie,
  NULL AS retour_emploi,
  NULL AS portefeuille,
  NULL AS sortie_anticipee,
  NULL AS sorties_anticipees_hors_sorties_positives,
  NULL AS recu,
  CAST(FORMAT_DATE('%Y%m', PARSE_DATE('%Y-%m-%d', LEFT(date_deb, 10))) AS INT64) AS annee_mois_sort,
  NULL AS retour_emploi_env_semap,
  CAST(NULL AS FLOAT64) AS Montant_prime,
  CAST(NULL AS DATE) AS Date_primes,
  NULL AS prime,
  NULL AS rnk_lc,
  NULL AS dont_nb_sessions,
  NULL AS note_globale,
  NULL AS nb_de_note,
  NULL AS dont_sessions_annulee,
  NULL AS total_nb_heure,
  NULL AS nb_pmsmp

FROM {{ref("stg_rdv")}}
WHERE type = "Réservé plateforme"
