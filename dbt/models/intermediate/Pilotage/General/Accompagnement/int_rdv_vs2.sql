WITH base AS (
  SELECT
    instance,
    id_rdv,
    id_benef,
    date_deb,
    date_fin,
    type,
    typage,
    objet,
    referent,
    id_referent,
    SPLIT(lot, ' ')[SAFE_OFFSET(0)] AS marche,
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
    ) AS lot_unique
  FROM {{ ref('stg_rdv') }}
  WHERE date_deb BETWEEN "2026-01-01" AND "2026-12-31"
    AND lot LIKE "VS2%"
),

with_session AS (
  SELECT
    *,
    FARM_FINGERPRINT(CONCAT(
      CAST(date_deb AS STRING), '|',
      CAST(date_fin AS STRING), '|',
      COALESCE(type, ''), '|',
      COALESCE(typage, ''), '|',
      COALESCE(objet, ''), '|',
      COALESCE(referent, ''), '|',
      COALESCE(CAST(id_referent AS STRING), ''), '|',
      COALESCE(marche, ''), '|',
      COALESCE(region, ''), '|',
      COALESCE(lot_unique, '')
    )) AS id_session
  FROM base
)

SELECT
    instance,
  id_benef,
  id_session,
  COUNT(id_rdv) OVER (PARTITION BY id_session) AS nombre_DE_par_session,
  date_deb,
  date_fin,
  type,
  typage,
  objet,
  referent,
  id_referent,
  marche,
  region,
  lot_unique
FROM with_session
