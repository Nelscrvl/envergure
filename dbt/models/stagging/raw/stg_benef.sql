with source as (

    select * from {{ source('sie_emploi', 'benef') }}

),

renamed as (

    select
        id_benef,
        civilite,
        nom,
        prenom,
        ide,
        tel1,
        tel2,
        email,
        commentaire,
        agence,
        conseiller,
        telconseiller,
        mailconseiller,
        referent,
        id_ref_principal,
        numero_commande,
        date_demarrage,
        date_debut_csp,
        date_fin_csp,
        date_fin_theo,
        date_fin_prest,
        lot,
        site,
        id_site,
        organisme,
        adresse_benef,
        cp_benef,
        ville_benef,
        insee_adresse,
        statut,
        situation,
        suivi,
        date_naissance,
        nom_naissance,
        info_adh,
        date_dern_modif,
        util_dern_modif,
        id_util_dern_modif,
        date_crea,
        util_crea,
        id_util_crea,
        nir,
        groupe,
        util_exclu_factu,
        date_exclu_factu,
        jobready,
        web_100,
        cdp,
        cdp_util,
        cdp_placement,
        acc_deld,
        emargement_impossible,
        prime,
        modalite_accompagnement,
        _extracted_at

    from source

)

select * from renamed