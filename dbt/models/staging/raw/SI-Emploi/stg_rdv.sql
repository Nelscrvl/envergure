with source as (

    select * from {{ source('sie_emploi', 'rdv') }}
    -- dt_rdv est chargée en append incrémental : chaque modification d'un RDV
    -- ajoute une nouvelle version. On ne garde que la plus récente.
    qualify row_number() over (
        partition by instance, id_rdv
        order by _watermark desc, _extracted_at desc
    ) = 1

),

organismes as (

    select * from {{ ref('seed_organisme') }}

),

marches as (

    select upper(trim(cle)) as cle, marche from {{ ref('seed_marche') }}

),

lots as (

    select *, {{ normalize_lot('lot') }} as lot_normalise from source

),

renamed as (

    select
        instance,
        id_benef,
        id_rdv,
        format_datetime('%Y-%m-%d %H:%M:%S', date_deb) as date_deb,
        format_datetime('%Y-%m-%d %H:%M:%S', date_fin) as date_fin,
        type,
        CASE
            WHEN LOWER(typage) IN ("entretien de diagnostic","entretien de validation")
            THEN "Entretien de Diagnostic"
            ELSE typage
        END AS typage,
        objet,
        referent,
        id_referent,
        site,
        source.lot_normalise as lot,
        source.lot as lot_source,
        {{ marche_from_lot('source.lot_normalise', 'm_lot', 'm_mot') }} as marche,
        id_site,
        -- colonne livrée par Sirom le 12/10/2026 pour la livraison des commandes FT
        coalesce(o.organisme, regexp_replace(trim(source.organisme), r'\s+', ' ')) as organisme,
        source.organisme as organisme_source,
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
        _extracted_at,


    from lots as source
    left join organismes as o
        on o.cle_organisme = {{ organisme_cle('source.organisme') }}
    left join marches as m_lot
        on m_lot.cle = upper(trim(source.lot_normalise))
    left join marches as m_mot
        on m_mot.cle = upper({{ premier_mot_lot('source.lot_normalise') }})

)

select * from renamed
