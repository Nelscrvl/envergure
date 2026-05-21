WITH prep_data AS (

SELECT
  f.*,
  pg.lot,
  pg.organisme,
  pg.numero_commande,
CASE
  WHEN split(pg.lot, ' ')[safe_offset(0)] = ''
  THEN SUBSTR(pg.numero_commande, 3, 3)
  ELSE split(pg.lot, ' ')[safe_offset(0)]
END AS marche,
CASE
  -- Outre-mer (noms uniques, pas d'ambiguïté)
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bMAYOTTE\b')                              THEN 'MAYOTTE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bGUADELOUPE\b|\bGUAD\b')                 THEN 'GUADELOUPE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bMARTINIQUE\b')                           THEN 'MARTINIQUE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bRÉUNION\b|\bREUNION\b|\bRN\b')          THEN 'REUNION'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bRM\b')                                   THEN 'REUNION-MARTINIQUE'

  -- Patterns multi-mots en premier (avant les abbréviations courtes)
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bGRAND[\s_-]?EST\b|\bCHAMPAGNE\b|\bLORRAINE\b|\bALSACE\b') THEN 'GRAND EST'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bHAUTS[\s-]DE[\s-]FRANCE\b')             THEN 'HAUTS-DE-FRANCE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bNOUVELLE[\s-]AQUITAINE\b')              THEN 'NOUVELLE-AQUITAINE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bCENTRE[\s-]VAL\b|\bEURE ET LOIR\b')    THEN 'CENTRE-VAL DE LOIRE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bBOURGOGNE\b|\bFRANCHE[\s-]COMT')       THEN 'BOURGOGNE-FRANCHE-COMTE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bSEINE[\s-]MARITIME\b|\bNORMANDIE\b')   THEN 'NORMANDIE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bAUVERGNE\b')                            THEN 'AUVERGNE-RHONE-ALPES'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bPAYS[\s-]DE[\s-]LA[\s-]LOIRE\b')       THEN 'PAYS DE LA LOIRE'

  -- Abréviations courtes (word boundary \b essentiel)
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bGDEST\b|\bGE\b')                        THEN 'GRAND EST'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bBFC\b|\bFC\b|\bBG\b')                   THEN 'BOURGOGNE-FRANCHE-COMTE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bNOR\b|\bNORM\b|\bHN\b')                 THEN 'NORMANDIE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bBRETAGNE\b')                            THEN 'BRETAGNE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bHDF\b')                                  THEN 'HAUTS-DE-FRANCE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bIDF\b')                                  THEN 'ILE-DE-FRANCE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bPACA\b')                                 THEN 'PACA'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bPDL\b')                                  THEN 'PAYS DE LA LOIRE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bNAQ\b')                                  THEN 'NOUVELLE-AQUITAINE'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bAURA\b|\bARA\b')                        THEN 'AUVERGNE-RHONE-ALPES'
  WHEN REGEXP_CONTAINS(pg.lot, r'(?i)\bCVL\b|\bCENTRE\b')                      THEN 'CENTRE-VAL DE LOIRE'

  ELSE NULL
END AS region,
COALESCE(
  REGEXP_EXTRACT(pg.lot, r'(?i)(?:^|[\s_])LOT[_\s]*(\d{1,3})(?:[_\s]|$)'),
  REGEXP_EXTRACT(pg.lot, r'[A-Za-z]+\s+(\d{1,2})\s+-\s+\d{5}$'),
  REGEXP_EXTRACT(pg.lot, r'^\S+\s+\S+\s+(\d{1,2})(?:\s|$)')
) AS lot_unique,
TRIM(SUBSTR(pg.lot, STRPOS(pg.lot, ' ') + 1)) AS lot_sans_action,
CAST(FORMAT_DATE('%Y%m', f.date_facture) AS INT64) AS annee_mois_factu

FROM {{ref('stg_facture')}} AS f
LEFT JOIN {{ref('stg_benef')}} AS pg
  ON f.id_benef = pg.id_benef
  AND f.instance = pg.instance

),

filtered AS (
  SELECT * FROM prep_data
  WHERE type_facture = 'solde'
    AND (num_origine IS NULL OR num_origine = '')

  UNION ALL

  SELECT * FROM prep_data
  WHERE type_facture = 'avoir'
    AND num_origine IS NOT NULL
    AND num_origine != ''
    AND num_origine != num_facture
)

SELECT
  instance,
  organisme,
  marche,
  lot,
  lot_unique,
  lot_sans_action,
  region,
  type_facture,
  SUM(IF(annee_mois_factu = 202601, montant_HT, 0)) AS `Janvier 2026`,
  SUM(IF(annee_mois_factu = 202602, montant_HT, 0)) AS `Février 2026`,
  SUM(IF(annee_mois_factu = 202603, montant_HT, 0)) AS `Mars 2026`,
  SUM(IF(annee_mois_factu = 202604, montant_HT, 0)) AS `Avril 2026`,
  SUM(IF(annee_mois_factu = 202605, montant_HT, 0)) AS `Mai 2026`,
  SUM(IF(annee_mois_factu = 202606, montant_HT, 0)) AS `Juin 2026`,
  SUM(IF(annee_mois_factu = 202607, montant_HT, 0)) AS `Juillet 2026`,
  SUM(IF(annee_mois_factu = 202608, montant_HT, 0)) AS `Août 2026`,
  SUM(IF(annee_mois_factu = 202609, montant_HT, 0)) AS `Septembre 2026`,
  SUM(IF(annee_mois_factu = 202610, montant_HT, 0)) AS `Octobre 2026`,
  SUM(IF(annee_mois_factu = 202611, montant_HT, 0)) AS `Novembre 2026`,
  SUM(IF(annee_mois_factu = 202612, montant_HT, 0)) AS `Décembre 2026`
FROM filtered
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
