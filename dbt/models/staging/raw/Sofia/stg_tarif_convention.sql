with source as (
    select
        IDConvent,
        IDSoc,
        IDTarifConvention,
        _id,
        Numero_Tarif,
        Nom_Type_Tarif,
        Nom_Tarif,
        Principal,
        Presentiel,
        Distanciel,
        Date_Debut,
        Date_Fin,
        Nombre_Stagiaire_Min,
        Nombre_Stagiaire_Max,
        Nombre_Heures_Min,
        Nombre_Heures_Max,
        Ne_Depasse_Pas_Heures,
        Ne_Depasse_Pas_Quantite,
        Compte_Heures_Dans_Stats,
        Tarif_Valable_Pour_Stagiaire,
        Tarif_Valable_Pour_Domaine,
        Observation,
        Type_Tarif,
        _extracted_at
    from {{ source('sofia', 'TarifConvention') }}
),

renamed as (
    select
        -- Identifiants
        IDConvent                       as id_convention,
        IDSoc                           as id_societe,
        IDTarifConvention               as id_tarif_convention,
        _id                             as internal_id,
        Numero_Tarif                    as numero_tarif,

        -- Libellés
        Nom_Type_Tarif                  as nom_type_tarif,
        Nom_Tarif                       as nom_tarif,

        -- Modalités
        Principal                       as est_principal,
        Presentiel                      as est_presentiel,
        Distanciel                      as est_distanciel,

        -- Validité
        Date_Debut                      as date_debut,
        Date_Fin                        as date_fin,

        -- Contraintes stagiaires
        Nombre_Stagiaire_Min            as nb_stagiaire_min,
        Nombre_Stagiaire_Max            as nb_stagiaire_max,

        -- Contraintes heures
        Nombre_Heures_Min               as nb_heures_min,
        Nombre_Heures_Max               as nb_heures_max,
        Ne_Depasse_Pas_Heures           as ne_depasse_pas_heures,
        Ne_Depasse_Pas_Quantite         as ne_depasse_pas_quantite,
        Compte_Heures_Dans_Stats        as compte_heures_dans_stats,

        -- Périmètre d'application
        Tarif_Valable_Pour_Stagiaire    as tarif_valable_pour_stagiaire,
        Tarif_Valable_Pour_Domaine      as tarif_valable_pour_domaine,
        Observation                     as observation,

        -- Type_Tarif → prix unitaires (préfixe tt_)
        Type_Tarif.Prix_Heure                       as tt_prix_heure,
        Type_Tarif.Prix_Journee                     as tt_prix_journee,
        Type_Tarif.Prix_DJ                          as tt_prix_dj,
        Type_Tarif.Prix                             as tt_prix,
        Type_Tarif.Prix_Stagiaire_Centre            as tt_prix_stagiaire_centre,
        Type_Tarif.Prix_Stagiaire_Entreprise        as tt_prix_stagiaire_entreprise,
        Type_Tarif.Prix_Stagiaire_Supplement        as tt_prix_stagiaire_supplement,
        Type_Tarif.Prix_Stagiaire_Visite            as tt_prix_stagiaire_visite,

        -- Type_Tarif → montants calculés
        Type_Tarif.Montant_Total                    as tt_montant_total,
        Type_Tarif.Montant_Global                   as tt_montant_global,
        Type_Tarif.Montant_Heure                    as tt_montant_heure,
        Type_Tarif.Montant_Journee                  as tt_montant_journee,
        Type_Tarif.Montant_DJ                       as tt_montant_dj,
        Type_Tarif.Montant                          as tt_montant,
        Type_Tarif.Montant_Supplement               as tt_montant_supplement,
        Type_Tarif.Montant_Visite                   as tt_montant_visite,
        Type_Tarif.Montant_Centre                   as tt_montant_centre,
        Type_Tarif.Montant_Entrep                   as tt_montant_entrep,

        -- Type_Tarif → durées prévisionnelles
        Type_Tarif.Duree_Prevue_Heures              as tt_duree_prevue_heures,
        Type_Tarif.Duree_Prevue_Jours               as tt_duree_prevue_jours,
        Type_Tarif.Duree_Totale                     as tt_duree_totale,
        Type_Tarif.Duree_Centre                     as tt_duree_centre,
        Type_Tarif.Duree_Entrep                     as tt_duree_entrep,
        Type_Tarif.Duree_Stagiaire_Centre           as tt_duree_stagiaire_centre,
        Type_Tarif.Duree_Stagiaire_Entreprise       as tt_duree_stagiaire_entreprise,

        -- Type_Tarif → quantités
        Type_Tarif.Quantite                         as tt_quantite,
        Type_Tarif.Quantite_Heure                   as tt_quantite_heure,
        Type_Tarif.Quantite_Journee                 as tt_quantite_journee,
        Type_Tarif.Quantite_DJ                      as tt_quantite_dj,
        Type_Tarif.Nombre_Stagiaire_Prevu           as tt_nombre_stagiaire_prevu,
        Type_Tarif.Nombre_Stagiaire_Min             as tt_nombre_stagiaire_min,
        Type_Tarif.Nombre_Stagiaire_Max             as tt_nombre_stagiaire_max,
        Type_Tarif.Nombre_Visites                   as tt_nombre_visites,

        -- Type_Tarif → règles de facturation
        Type_Tarif.Facturation                      as tt_facturation,
        Type_Tarif.Facturer_Si_Heures_Planif        as tt_facturer_si_heures_planif,
        Type_Tarif.Facturer_Abs_Pas_Pres            as tt_facturer_abs_pas_pres,
        Type_Tarif.Pas_Tenu_Compte_Abs_Fact         as tt_pas_tenu_compte_abs_fact,
        Type_Tarif.Inclure_Pres_Entrep              as tt_inclure_pres_entrep,
        Type_Tarif.Fact_Une_Fois_Seances_Concomitantes as tt_fact_une_fois_seances_concomitantes,
        Type_Tarif._id                              as tt_internal_id,

        _extracted_at

    from source
)

select * from renamed
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_tarif_convention, id_societe
    ORDER BY _extracted_at DESC
) = 1