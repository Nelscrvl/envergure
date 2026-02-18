with source as (
    select * from {{ source('sofia', 'Inscrite_Soc_3') }}
),

renamed as (
    SELECT
        -- Colonnes racines
        source.Code_Analytique_Parcours,
        source.Type_Region,
        source.Code_Region,
        source.Libelle_Court_Parcours,
        source.Libelle_Parcours_Groupe,
        source.ID,
        source.Libelle_Parcours,
        source.IDAction,    
        source.CIR,
        format_datetime('%Y-%m-%d %H:%M:%S',source.Date_Entree ) As Date_Entree,
        source.Convention,
        source.Date_Sortie,
        source.Type_apprentissage,
        source.IDParcours_Groupe,  
        format_datetime('%Y-%m-%d %H:%M:%S',source.Date_Sortie_Previsionnelle) AS Date_Sortie_Previsionnelle,
        
        -- Informations du formateur référent (préfixe fr_)
        fr.ID as fr_formateur_id,
        fr._id as fr_formateur_internal_id, 
        fr.ID_Individu as fr_individu_id,
        fr.Civilite as fr_civilite,
        fr.Nom as fr_nom,
        fr.Prenom as fr_prenom,
        fr.NomJF as fr_nom_jeune_fille,
        fr.Date_Naissance as fr_date_naissance,
        
        -- Coordonnées professionnelles formateur (préfixe fr_)
        fr.Email_Pro as fr_email_pro,
        fr.Telephone_Pro as fr_telephone_pro,
        fr.Mobile_Pro as fr_mobile_pro,
        
        -- Coordonnées personnelles formateur (préfixe fr_)
        fr.Email_Perso as fr_email_perso,
        fr.Telephone_Perso as fr_telephone_perso,
        fr.Mobile_Perso as fr_mobile_perso,
        
        -- Adresse formateur (préfixe fr_)
        fr.Adresse.ID as fr_adresse_id,
        fr.Adresse._id as fr_adresse_internal_id,
        fr.Adresse.NumRue as fr_numero_rue,
        fr.Adresse.Adr1 as fr_adresse_ligne1,
        fr.Adresse.Adr2 as fr_adresse_ligne2,
        fr.Adresse.Adr3 as fr_adresse_ligne3,
        fr.Adresse.Adr4 as fr_adresse_ligne4,
        fr.Adresse.Code_Postal as fr_code_postal,
        fr.Adresse.Code_Insee as fr_code_insee,
        fr.Adresse.Ville as fr_ville,
        fr.Adresse.Arrondissement as fr_arrondissement,
        fr.Adresse.Pays as fr_pays,
        
        -- Informations du stagiaire (préfixe stg_)
        stg.ID as stg_stagiaire_id,
        stg._id as stg_stagiaire_internal_id,
        stg.ID_Individu as stg_individu_id,
        stg.Reference as stg_reference,
        stg.Civilite as stg_civilite,
        stg.Nom as stg_nom,
        stg.Prenom as stg_prenom,
        stg.Nom_Naissance as stg_nom_naissance,
        stg.Date_Naissance as stg_date_naissance,
        stg.Identifiant_Utilisateur as stg_identifiant_utilisateur,
        stg.Charge_Affaires as stg_charge_affaires,
        
        -- Coordonnées professionnelles stagiaire (préfixe stg_)
        stg.Email_Pro as stg_email_pro,
        stg.Telephone_Pro as stg_telephone_pro,
        stg.Mobile_Pro as stg_mobile_pro,
        
        -- Coordonnées personnelles stagiaire (préfixe stg_)
        stg.Email_Perso as stg_email_perso,
        stg.Telephone_Perso as stg_telephone_perso,
        stg.Mobile_Perso as stg_mobile_perso,
        
        -- Adresse stagiaire (préfixe stg_)
        stg.Adresse.ID as stg_adresse_id,
        stg.Adresse._id as stg_adresse_internal_id,
        stg.Adresse.NumRue as stg_numero_rue,
        stg.Adresse.Adr1 as stg_adresse_ligne1,
        stg.Adresse.Adr2 as stg_adresse_ligne2,
        stg.Adresse.Adr3 as stg_adresse_ligne3,
        stg.Adresse.Adr4 as stg_adresse_ligne4,
        stg.Adresse.Code_Postal as stg_code_postal,
        stg.Adresse.Code_Insee as stg_code_insee,
        stg.Adresse.Ville as stg_ville,
        stg.Adresse.Arrondissement as stg_arrondissement,
        stg.Adresse.Pays as stg_pays
        
    FROM source,
    UNNEST([source.formateur_referent]) as fr,
    UNNEST([source.stagiaire]) as stg
    

)

select * from renamed