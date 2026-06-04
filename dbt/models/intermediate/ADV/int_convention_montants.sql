with soc_2 as (
    select
        conv.IDConvent,
        conv._id                        as convention_internal_id,
        conv.Reference                  as convention_reference,
        conv.Numero_interne             as convention_numero_interne,
        conv.Numero_financeur           as convention_numero_financeur,

        mc.MontantTTC                   as montant_ttc,
        mc.MontantTVA                   as montant_tva_montant,
        mc.MontantHT                    as montant_ht,
        mc._id                          as montant_id,
        mc.TVA.TauxTVA                  as montant_tva_taux,
        mc.TVA.IDTVA                    as montant_tva_id,
        mc.TVA._id                      as montant_tva_internal_id,

        'Convention_Soc_2'              as source_table

    from {{ source('sofia', 'Convention_Soc_2') }} as conv
    cross join unnest(conv.Montants_Convention) as mc
),

soc_4 as (
    select
        conv.IDConvent,
        conv._id                        as convention_internal_id,
        conv.Reference                  as convention_reference,
        conv.Numero_interne             as convention_numero_interne,
        conv.Numero_financeur           as convention_numero_financeur,

        mc.MontantTTC                   as montant_ttc,
        mc.MontantTVA                   as montant_tva_montant,
        mc.MontantHT                    as montant_ht,
        mc._id                          as montant_id,
        mc.TVA.TauxTVA                  as montant_tva_taux,
        mc.TVA.IDTVA                    as montant_tva_id,
        mc.TVA._id                      as montant_tva_internal_id,

        'Convention_Soc_4'              as source_table

    from {{ source('sofia', 'Convention_Soc_4') }} as conv
    cross join unnest(conv.Montants_Convention) as mc
)

select * from soc_2
union all
select * from soc_4
