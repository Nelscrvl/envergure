-- Modèle de debug temporaire — à supprimer après investigation
-- Compte les stagiaires à chaque étape du pipeline pour une session donnée

WITH etape_1a AS (
    SELECT 'stg_inscrite_Soc_2' AS etape, COUNT(DISTINCT stg_stagiaire_id) AS nb_stagiaires
    FROM {{ ref('stg_inscrite_Soc_2') }}
    WHERE conv_client_nom LIKE 'Les Compa%'
      AND Date_Entree = '2026-04-28 00:00:00'
),

etape_1b AS (
    SELECT 'stg_inscrite_Soc_3' AS etape, COUNT(DISTINCT stg_stagiaire_id) AS nb_stagiaires
    FROM {{ ref('stg_inscrite_Soc_3') }}
    WHERE conv_client_nom LIKE 'Les Compa%'
      AND Date_Entree = '2026-04-28 00:00:00'
),

etape_1c AS (
    SELECT 'stg_inscrite_Soc_4' AS etape, COUNT(DISTINCT stg_stagiaire_id) AS nb_stagiaires
    FROM {{ ref('stg_inscrite_Soc_4') }}
    WHERE conv_client_nom LIKE 'Les Compa%'
      AND Date_Entree = '2026-04-28 00:00:00'
),

etape_2 AS (
    SELECT 'Int_inscrit_formation' AS etape, COUNT(DISTINCT stg_stagiaire_id) AS nb_stagiaires
    FROM {{ ref('Int_inscrit_formation') }}
    WHERE conv_client_nom LIKE 'Les Compa%'
      AND Date_Entree = '2026-04-28 00:00:00'
),

etape_3 AS (
    SELECT 'int_inscrit_pilotage' AS etape, COUNT(DISTINCT stg_stagiaire_id) AS nb_stagiaires
    FROM {{ ref('int_inscrit_pilotage') }}
    WHERE conv_client_nom LIKE 'Les Compa%'
      AND date_entree = '2026-04-28 00:00:00'
)

SELECT * FROM etape_1a
UNION ALL SELECT * FROM etape_1b
UNION ALL SELECT * FROM etape_1c
UNION ALL SELECT * FROM etape_2
UNION ALL SELECT * FROM etape_3
ORDER BY etape
