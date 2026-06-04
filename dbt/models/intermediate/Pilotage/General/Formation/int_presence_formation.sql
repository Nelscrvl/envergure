WITH union_all AS 
(
    SELECT
*
FROM {{ref("stg_presence_Soc_2")}}

UNION ALL 

SELECT
*
FROM {{ref("stg_presence_Soc_3")}}

UNION ALL 

SELECT
*
FROM {{ref("stg_presence_Soc_4")}}

)

SELECT
seance_libelle_court
type_seance,
libelle_type_seance,
TIME_DIFF(duree_seance, TIME(0,0,0), MINUTE) as duree_seance_minutes,
TIME_DIFF(duree_seance, TIME(0,0,0), MINUTE) / 60.0 as duree_seance_heures,
duree_seance,
heure_debut,
heure_fin_seance,
distancielle,
personnalisee,
duree_absence,
TIME_DIFF(duree_absence, TIME(0,0,0),MINUTE)/60 as duree_absence_heures,
debut_absence,
fin_absence,
type_region,
libelle_parcours,
libelle_court_parcours,
libelle_court_parcours_groupe,
code_analytique_parcours,
date_entree,
date_sortie,
stagiaire_id,
stagiaire_nom,
source_libelle,
source_libelle_lieu,
intervenant_id,
intervenant_reference,
intervenant_nom,
source_debut,
source_fin,
source_duree,
id_module,
date_date
FROM union_all