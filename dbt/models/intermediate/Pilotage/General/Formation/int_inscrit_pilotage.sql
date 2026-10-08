with inscrits as (
    select * from {{ ref('Int_inscrit_formation') }}
)

select
    -- Identifiants
    i.stg_stagiaire_id,
    i.Code_Analytique_Parcours                                              as code_analytique_parcours,
    i.specialite_code,
    i.specialite_libelle,
    i.Libelle_Court_Parcours                                                as libelle_parcours,
    i.IDParcours_Groupe                                                     as id_parcours_groupe,
    i.Type_Region                                                           as type_region,
    i.conv_id                                                               as id_convention,
    i.conv_id_societe,
    i.Date_Entree                                                           as date_entree,
    EXTRACT(MONTH FROM SAFE_CAST(LEFT(i.Date_Entree, 10) AS DATE))         as mois_entree,
    i.Date_Sortie                                                           as date_sortie,
    i.Date_Sortie_Previsionnelle                                            as date_sortie_previsionnelle,

    -- Stagiaire contact
    i.id_societe,
    i.stg_email_pro,
    i.stg_email_perso,

    -- Formateur
    i.fr_formateur_id,
    i.formateur_nom_complet,
    i.est_formateur_externe,
    IF(i.a_passe_examen,  1, 0)                                            as a_passe_examen,
    IF(i.a_reussi_examen, 1, 0)                                            as a_reussi_examen,
    i.note_satisfaction,

    -- Convention — identifiants et dimensions
    i.conv_numero_financeur,
    i.conv_libelle2,
    i.conv_client_id,
    i.conv_client_nom,
    i.groupe_ou_individuelle,
    i.date_debut_convention,
    i.date_fin_convention,
    i.duree_convention_jours,

    -- Tarif
    i.nom_type_tarif,
    i.tt_prix,
    i.tt_prix_journee,
    i.tt_montant_journee,
    i.tt_quantite_journee,
    i.prix_stagiaire_centre,
    i.prix_stagiaire_entrep,
    i.nb_inscrits_groupe,
    i.nb_inscrits,

    -- BDC / Objectifs
    i.montant_total_bdc,
    i.montant_centre_bdc,
    i.montant_entrep_bdc,
    -- Pas de repli sur duree_prevue_heures_bdc ici : il rendait cette colonne
    -- rigoureusement egale a heures_realisees (inscrits x duree contractuelle).
    -- Le prevu au niveau groupe est heures_conventionnees_groupe (places x duree).
    i.duree_stagiaire_centre_bdc                                            as heures_centre_prevues,
    i.duree_stagiaire_entrep_bdc                                            as heures_entrep_prevues,
    COALESCE(
        NULLIF(COALESCE(i.duree_stagiaire_centre_bdc, 0)
             + COALESCE(i.duree_stagiaire_entrep_bdc, 0), 0),
        i.duree_prevue_heures_bdc
    )                                                                       as heures_totales_prevues,
    i.duree_prevue_heures_bdc,
    i.nb_stagiaire_prevu,
    SAFE_DIVIDE(CAST(i.nb_stagiaire_prevu AS NUMERIC), i.nb_inscrits) AS nb_stagiaire_prevu_prorata,
    CASE
        WHEN i.nom_type_tarif = 'Forfait groupe (ou forfait formateur)'
        THEN SAFE_DIVIDE(i.montant_total_bdc, i.nb_inscrits_groupe)
        ELSE SAFE_DIVIDE(i.montant_total_bdc, i.nb_inscrits)
    END                                                                     as ca_prevu_par_inscrit,

    -- Heures / jours réalisés
    i.nb_jours_ouvres,
    -- heures_realisees somme les durees de seance sans deduire l'absence : c'est
    -- du programme. heures_effectives est le temps reellement suivi.
    i.heures_realisees,
    i.heures_absence,
    i.heures_realisees - i.heures_absence                                   as heures_effectives,
    i.heures_stage,
    i.heures_formateur,
    i.heures_realisees + i.heures_stage                                     as heures_totales,

    -- CA réel
    i.ca_centre,
    i.ca_entrep,
    CASE
        WHEN i.nom_type_tarif = 'Forfait groupe (ou forfait formateur)'
        THEN SAFE_DIVIDE(i.ca_total, i.nb_inscrits_groupe)
        ELSE i.ca_total
    END                                                                     as ca_genere,

    -- KPIs de réalisation
    SAFE_DIVIDE(i.heures_realisees, i.duree_stagiaire_centre_bdc) * 100    as taux_realisation_centre,
    SAFE_DIVIDE(i.heures_stage,     i.duree_stagiaire_entrep_bdc) * 100    as taux_realisation_entrep,
    SAFE_DIVIDE(
        i.heures_realisees + i.heures_stage,
        COALESCE(i.duree_stagiaire_centre_bdc, 0)
            + COALESCE(i.duree_stagiaire_entrep_bdc, 0)
    ) * 100                                                                 as taux_realisation_total,

    -- Flags (0/1)
    -- Abandon = sortie avant la date de sortie PREVUE du stagiaire.
    -- L'ancienne regle comparait a la fin de CONVENTION : sur les conventions
    -- multi-groupes (FT IDF, CR CVDL...) la convention court des mois apres la fin
    -- normale d'un groupe, et tous ses stagiaires etaient comptes en abandon
    -- (2 163 abandons sur 5 015 inscriptions, contre 287 sorties reellement anticipees).
    IF(DATE(i.Date_Sortie) < SAFE_CAST(LEFT(i.Date_Sortie_Previsionnelle, 10) AS DATE)
       AND DATE(i.Date_Sortie) <= CURRENT_DATE(),
       1, 0)                                                                as abandon_parcours,
    -- Date a laquelle l'abandon survient, pour l'attribuer au bon mois d'activite.
    IF(DATE(i.Date_Sortie) < SAFE_CAST(LEFT(i.Date_Sortie_Previsionnelle, 10) AS DATE)
       AND DATE(i.Date_Sortie) <= CURRENT_DATE(),
       DATE(i.Date_Sortie), NULL)                                          as date_abandon,
    IF(i.heures_realisees >= COALESCE(i.duree_stagiaire_centre_bdc, 0),
       1, 0)                                                                as a_realise_heures_centre,
    IF(i.heures_stage >= COALESCE(i.duree_stagiaire_entrep_bdc, 0),
       1, 0)                                                                as a_realise_heures_entrep,
    IF(i.ca_total >= SAFE_DIVIDE(i.montant_total_bdc, i.nb_inscrits),
       1, 0)                                                                as a_genere_assez_ca,
    IF(i.heures_realisees > 0, 1, 0)                                       as a_demarre_formation,
    IF(i.conv_id IS NOT NULL, 1, 0)                                        as est_conventionne,
    -- Attention : "en cours a la date du jour". Pour savoir qui etait en cours sur
    -- un mois passe, utiliser mrt_inscrit_pilotage_stock et sa colonne mois_actif.
    IF(SAFE_CAST(i.Date_Sortie AS DATE) >= CURRENT_DATE(), 1, 0)           as est_en_cours,

    -- ---------------------------------------------------------------------
    -- Colonnes de groupe, sommables sous n'importe quel filtre
    --
    -- Les attributs de convention ou d'action (effectif prevu, heures
    -- conventionnees, jours de formation) sont repartis au PRORATA des inscriptions
    -- du groupe : chaque ligne en porte sa part. La somme sur le groupe est exacte,
    -- et un filtre mensuel ou regional en recupere la part correspondante.
    --
    -- La version precedente portait la valeur entiere sur UNE seule ligne : sous un
    -- filtre mensuel, une convention dont les entrees s'etalent sur plusieurs mois
    -- (57 conventions, 1 540 inscriptions) se retrouvait entierement dans un mois et
    -- absente des autres, d'ou des ecarts prevus/inscrits incomprehensibles.
    -- ---------------------------------------------------------------------
    1                                                                       as nb_stagiaires_groupe,

    SAFE_DIVIDE(i.nb_jours_ouvres,
                COUNT(*) OVER (PARTITION BY i.IDAction))                    as nb_jours_formation_groupe,

    SAFE_DIVIDE(i.nb_stagiaire_prevu,
                COUNT(*) OVER (PARTITION BY i.conv_id, i.conv_id_societe))  as nb_stagiaire_prevu_groupe,

    -- Heures conventionnees : places prevues x duree par stagiaire, au prorata.
    SAFE_DIVIDE(i.heures_conventionnees_bdc,
                COUNT(*) OVER (PARTITION BY i.conv_id, i.conv_id_societe))  as heures_conventionnees_groupe,

    -- Paire dediee au taux de saturation : restreinte aux conventions dont
    -- l'effectif prevu est connu, pour que numerateur et denominateur portent sur
    -- le meme perimetre. Taux = SUM(saturation_inscrits) / SUM(saturation_prevus).
    IF(i.nb_stagiaire_prevu IS NOT NULL, 1, NULL)                          as saturation_inscrits,

    IF(i.nb_stagiaire_prevu IS NOT NULL,
       SAFE_DIVIDE(i.nb_stagiaire_prevu,
                   COUNT(*) OVER (PARTITION BY i.conv_id, i.conv_id_societe)),
       NULL)                                                               as saturation_prevus,

    -- CA potentiel par inscrit. Le BDC ne porte de montant que sur une partie des
    -- tarifs ; pour "Forfait stagiaire" le prix vit dans tt_prix, d'ou le repli.
    CASE
        -- Le forfait stagiaire est un prix par personne : le diviser par l'effectif
        -- reel le sous-estimerait des que le groupe depasse la capacite prevue.
        WHEN i.nom_type_tarif = 'Forfait stagiaire'
        THEN i.tt_prix
        WHEN i.nom_type_tarif = 'Forfait groupe (ou forfait formateur)'
        THEN SAFE_DIVIDE(i.montant_total_bdc, i.nb_inscrits_groupe)
        ELSE SAFE_DIVIDE(i.montant_total_bdc, i.nb_inscrits)
    END                                                                    as ca_potentiel

from inscrits i
--WHERE  LIKE "Les Compa%"