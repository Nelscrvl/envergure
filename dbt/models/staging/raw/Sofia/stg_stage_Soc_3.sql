with source as (
    select * from {{ source('sofia', 'Stage_Soc_3') }}
),

renamed as (
    select
        ID,
        _id,
        IDAction,
        CodeAnalytique,
        Libelle,
        Libelle_Action,
        Libelle_Fonction,
        IDFonction,
        Libelle_Rome,
        CodeRome,
        IDRome,
        Archive,
        DateDebut,
        DateFin,
        Duree,
        _extracted_at,

        -- Visites (ARRAY)
        cast(to_json_string(Visites) as string) as visites,

        -- Stagiaire
        Stagiaire.ID            as stagiaire_id,
        Stagiaire._id           as stagiaire_internal_id,
        Stagiaire.Civilite      as stagiaire_civilite,
        Stagiaire.Nom           as stagiaire_nom,
        Stagiaire.Prenom        as stagiaire_prenom,
        Stagiaire.Email_Perso   as stagiaire_email_perso,
        Stagiaire.Mobile_Perso  as stagiaire_mobile_perso,
        Stagiaire.DNaiss        as stagiaire_date_naissance,

        -- Entreprise
        Entreprise.ID           as entreprise_id,
        Entreprise._id          as entreprise_internal_id,
        Entreprise.Reference    as entreprise_reference,
        Entreprise.Nom          as entreprise_nom,
        Entreprise.Sigle        as entreprise_sigle,
        Entreprise.Email        as entreprise_email,
        Entreprise.Telephone    as entreprise_telephone,
        Entreprise.Mobile       as entreprise_mobile,
        Entreprise.Adresse.Ville        as entreprise_ville,
        Entreprise.Adresse.Code_Postal  as entreprise_code_postal,
        Entreprise.Adresse.Code_Insee   as entreprise_code_insee,
        Entreprise.Adresse.Pays         as entreprise_pays,
        Entreprise.Adresse.Adr1         as entreprise_adresse_ligne1,

        -- TuteurDuCentre
        TuteurDuCentre.ID       as tuteur_centre_id,
        TuteurDuCentre._id      as tuteur_centre_internal_id,
        TuteurDuCentre.Civilite as tuteur_centre_civilite,
        TuteurDuCentre.Nom      as tuteur_centre_nom,
        TuteurDuCentre.Prenom   as tuteur_centre_prenom,
        TuteurDuCentre.Email    as tuteur_centre_email,
        TuteurDuCentre.Telephone as tuteur_centre_telephone,

        -- TuteurDeStage
        TuteurDeStage.ID        as tuteur_stage_id,
        TuteurDeStage._id       as tuteur_stage_internal_id,
        TuteurDeStage.Civilite  as tuteur_stage_civilite,
        TuteurDeStage.Nom       as tuteur_stage_nom,
        TuteurDeStage.Prenom    as tuteur_stage_prenom,
        TuteurDeStage.Email     as tuteur_stage_email,
        TuteurDeStage.Telephone as tuteur_stage_telephone,

        -- MaitreDeStage
        MaitreDeStage.ID        as maitre_stage_id,
        MaitreDeStage._id       as maitre_stage_internal_id,
        MaitreDeStage.Civilite  as maitre_stage_civilite,
        MaitreDeStage.Nom       as maitre_stage_nom,
        MaitreDeStage.Prenom    as maitre_stage_prenom,
        MaitreDeStage.Email     as maitre_stage_email,
        MaitreDeStage.Telephone as maitre_stage_telephone,

        -- Signataire
        Signataire.ID           as signataire_id,
        Signataire._id          as signataire_internal_id,
        Signataire.Civilite     as signataire_civilite,
        Signataire.Nom          as signataire_nom,
        Signataire.Prenom       as signataire_prenom,
        Signataire.Email        as signataire_email,
        Signataire.Telephone    as signataire_telephone,

        'Stage_Soc_3'           as source_table

    from source
)

select * from renamed
