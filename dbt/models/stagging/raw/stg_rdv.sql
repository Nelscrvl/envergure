with source as (

    select * from {{ source('sie_emploi', 'rdv') }}

),

renamed as (

    select
        id_benef,
        id_rdv,
        format_datetime('%Y-%m-%d %H:%M:%S', date_deb) as date_deb,
        format_datetime('%Y-%m-%d %H:%M:%S', date_fin) as date_fin,
        type,
        typage,
        objet,
        referent,
        id_referent,
        site,
        id_site,
        present,
        motif_absence,
        theme,
        module,
        dispo_rdv,
        commentaire,
        date_dern_modif,
        util_dern_modif,
        id_util_dern_modif,
        date_crea,
        util_crea,
        id_util_crea,
        mode_rdv,
        signature_envoi,
        signature_benef,
        precision_rdv,
        _extracted_at

    from source

)

select * from renamed