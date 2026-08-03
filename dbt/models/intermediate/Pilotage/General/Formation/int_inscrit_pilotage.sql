with inscrits as (
    select * from {{ ref('Int_inscrit_formation') }}
)

select
    -- Identifiants
    i.stg_stagiaire_id,
    i.Code_Analytique_Parcours                                              as code_analytique_parcours,
    i.Libelle_Court_Parcours                                                as libelle_parcours,
    i.IDParcours_Groupe                                                     as id_parcours_groupe,
    i.Type_Region                                                           as type_region,
    i.conv_id                                                               as id_convention,
    i.conv_id_societe,
    i.Date_Entree                                                           as date_entree,
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
    i.a_passe_examen,
    i.a_reussi_examen,
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
    i.duree_stagiaire_centre_bdc                                            as heures_centre_prevues,
    i.duree_stagiaire_entrep_bdc                                            as heures_entrep_prevues,
    COALESCE(i.duree_stagiaire_centre_bdc, 0)
        + COALESCE(i.duree_stagiaire_entrep_bdc, 0)                        as heures_totales_prevues,
    i.duree_prevue_heures_bdc,
    i.nb_stagiaire_prevu,
    CASE
        WHEN i.nom_type_tarif = 'Forfait groupe (ou forfait formateur)'
        THEN SAFE_DIVIDE(i.montant_total_bdc, i.nb_inscrits_groupe)
        ELSE SAFE_DIVIDE(i.montant_total_bdc, i.nb_inscrits)
    END                                                                     as ca_prevu_par_inscrit,

    -- Heures réalisées
    i.heures_realisees,
    i.heures_absence,
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
    IF(i.Date_Sortie IS NULL
       OR SAFE_CAST(i.Date_Sortie AS DATE) < SAFE_CAST(i.Date_Sortie_Previsionnelle AS DATE),
       1, 0)                                                                as abandon_parcours,
    IF(i.heures_realisees >= COALESCE(i.duree_stagiaire_centre_bdc, 0),
       1, 0)                                                                as a_realise_heures_centre,
    IF(i.heures_stage >= COALESCE(i.duree_stagiaire_entrep_bdc, 0),
       1, 0)                                                                as a_realise_heures_entrep,
    IF(i.ca_total >= SAFE_DIVIDE(i.montant_total_bdc, i.nb_inscrits),
       1, 0)                                                                as a_genere_assez_ca,
    IF(i.heures_realisees > 0, 1, 0)                                       as a_demarre_formation

from inscrits i
--WHERE  LIKE "Les Compa%"