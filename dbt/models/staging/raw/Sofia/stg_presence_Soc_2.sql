with source as (
    select * 
    from {{ source('sofia', 'Presence_Soc_2') }}
),

renamed as (
    select
        -- Champs à risque → normalisation
        cast(to_json_string(Materiels) as string) as materiels,

        -- =========================
        -- Séance
        -- =========================
        cast(seance.Libelle as string) as seance_libelle,
        cast(seance.Libelle_Court as string) as seance_libelle_court,
        cast(seance._id as string) as seance_internal_id,
        cast(seance.Module_Prevu as string) as module_prevu,
        cast(seance.Type_Seance as string) as type_seance,
        cast(seance.Libelle_Type_Seance as string) as libelle_type_seance,
        seance.Duree  as duree_seance,
        cast(seance.Debut as string) as heure_debut,
        cast(seance.Fin as string) as heure_fin_seance,
        seance.Distancielle as distancielle,
        seance.Personnalisee as personnalisee,
        seance.Seance_Action_Principale  as seance_action_principale,
        cast(seance.Signature as string) as signature,

        -- =========================
        -- Absence
        -- =========================
        cast(seance.LTypAbsen as string) as type_absence_libelle,
        SAFE_CAST(seance.IDTypAbsen as INT64) as type_absence_id,
        seance.Duree_Absence  as duree_absence,
        cast(seance.Debut_Absence as string) as debut_absence,
        cast(seance.Fin_Absence as string) as fin_absence,

        -- =========================
        -- Inscription
        -- =========================
        seance.Inscrit.ID  as inscription_id,
        cast(seance.Inscrit._id as string) as inscription_internal_id,
        cast(seance.Inscrit.Type_Region as string) as type_region,
        cast(seance.Inscrit.Code_Region as string) as code_region,
        cast(seance.Inscrit.Libelle_Parcours as string) as libelle_parcours,
        cast(seance.Inscrit.Libelle_Court_Parcours as string) as libelle_court_parcours,
        cast(seance.Inscrit.Libelle_Parcours_Groupe as string) as libelle_parcours_groupe,
        cast(seance.Inscrit.Libelle_Court_Parcours_Groupe as string) as libelle_court_parcours_groupe,
        cast(seance.Inscrit.Code_Analytique_Parcours as string) as code_analytique_parcours,
        seance.Inscrit.IDParcours_Groupe  as id_parcours_groupe,
        cast(seance.Inscrit.Date_Entree as date) as date_entree,
        cast(seance.Inscrit.Date_Sortie as date) as date_sortie,

        -- =========================
        -- Stagiaire
        -- =========================
        seance.Inscrit.Stagiaire.ID  as stagiaire_id,
        cast(seance.Inscrit.Stagiaire._id as string) as stagiaire_internal_id,
        seance.Inscrit.Stagiaire.ID_Individu  as stagiaire_individu_id,
        cast(seance.Inscrit.Stagiaire.Reference as string) as stagiaire_reference,
        cast(seance.Inscrit.Stagiaire.Civilite as string) as stagiaire_civilite,
        cast(seance.Inscrit.Stagiaire.Nom as string) as stagiaire_nom,
        cast(seance.Inscrit.Stagiaire.Prenom as string) as stagiaire_prenom,

        -- =========================
        -- Convention
        -- =========================
        seance.Inscrit.Convention.ID  as convention_id,
        cast(seance.Inscrit.Convention._id as string) as convention_internal_id,
        seance.Inscrit.Convention.IDSociete as convention_id_societe,
        cast(seance.Inscrit.Convention.Libelle as string) as convention_libelle,
        cast(seance.Inscrit.Convention.Libelle2 as string) as convention_libelle2,
        cast(seance.Inscrit.Convention.Reference as string) as convention_reference,
        cast(seance.Inscrit.Convention.Numero_interne as string) as convention_numero_interne,
        cast(seance.Inscrit.Convention.Numero_financeur as string) as convention_numero_financeur,

        -- =========================
        -- Client
        -- =========================
        seance.Inscrit.Convention.Client.ID  as client_id,
        cast(seance.Inscrit.Convention.Client._id as string) as client_internal_id,
        cast(seance.Inscrit.Convention.Client.Nom as string) as client_nom,
        cast(seance.Inscrit.Convention.Client.Reference as string) as client_reference,

        -- =========================
        -- Financeur
        -- =========================
        seance.Inscrit.Convention.Financeur.ID  as financeur_id,
        cast(seance.Inscrit.Convention.Financeur._id as string) as financeur_internal_id,
        cast(seance.Inscrit.Convention.Financeur.Nom as string) as financeur_nom,
        cast(seance.Inscrit.Convention.Financeur.Reference as string) as financeur_reference,

        -- =========================
        -- Autres champs source
        -- =========================
        cast(to_json_string(Salles) as string) as salles,
        cast(IDLieu as int64) as id_lieu,
        Visibilite_stagiaire  as visibilite_stagiaire,
        cast(Libelle_Action as string) as libelle_action,
        IDAction  as id_action,
        cast(source.Fin as string) as source_fin,
        cast(source.Libelle_Court as string) as source_libelle_court,
        source.Duree  as source_duree,
        cast(source.Libelle_Lieu as string) as source_libelle_lieu,
        cast(source.Libelle as string) as source_libelle,

        -- =========================
        -- Intervenant
        -- =========================
        interv.ID  as intervenant_id,
        cast(interv._id as string) as intervenant_internal_id,
        cast(interv.Reference as string) as intervenant_reference,
        cast(interv.Civilite as string) as intervenant_civilite,
        cast(interv.Nom as string) as intervenant_nom,
        cast(interv.Prenom as string) as intervenant_prenom,
        cast(interv.Signature as string) as intervenant_signature,

        -- =========================
        -- Dates & techniques
        -- =========================
        cast(source.Debut as string) as source_debut,
        IDModule as id_module,
        format_datetime('%Y-%m-%d %H:%M:%S', Date) as date_date,
        source.ID  as source_id,

        -- traçabilité
        'Presence_Soc_2' as source_table

    from source
    cross join unnest(Seances_persos) as seance
    cross join unnest(Intervenants) as interv
)

select * from renamed