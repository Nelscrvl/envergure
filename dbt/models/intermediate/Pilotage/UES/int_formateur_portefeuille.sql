SELECT 
    referent,
    marche,
    region,
    lot_unique,
    site,
    statut,
    COUNT(*) AS candidats_en_portefeuille
FROM {{ref("int_pilotage")}}
---WHERE date_demarrage <= CURRENT_DATE
---  AND (date_fin_prest IS NULL OR date_fin_prest >= CURRENT_DATE)
WHERE date_fin_prest IS NULL AND statut = "Prestation en cours"
GROUP BY referent, marche, region, lot_unique, statut, site
ORDER BY referent