{{ 
    config(
        materialized= "view",
        tags= ["silver", "mts_vw_tum_kpis"]
    )
}}


with mns_utilisation_equipement as (
    select * from {{ source('bronze', 'A_BRONZE_MNS_F_UTILISATION_EQUIPEMENT') }}
),

mns_f_equipement_arret as (
    select * from {{ source('silver', 'b_silver_mns_f_equipement_arret_v_0_1') }}
),

cte as (

	select 
         equ_arret.mea_msi_sk
        ,equ_arret.mea_msi_code
        ,equ_arret.mea_equ_sk as NeembaEquipmentID
        ,to_varchar(date_trunc('month', equ_arret.mea_date_debut_heure), 'YYYY-MM') as Month
        ,equ_arret.mea_date_debut_heure
        ,equ_arret.mea_date_fin_heure
        ,case 
            when equ_arret.mea_flag_planification = 'P' 
                 then datediff('second', equ_arret.mea_date_debut_heure, equ_arret.mea_date_fin_heure) / 3600 
            else 0 
         end as DateDiffTest
        ,equ_arret.mea_flag_planification
    from mns_f_equipement_arret equ_arret
    where equ_arret.mea_date_debut_heure is not null
      and equ_arret.mea_date_fin_heure is not null
      and datediff('hour', equ_arret.mea_date_debut_heure, equ_arret.mea_date_fin_heure) >= 0
),

EquipmentHours as (
	select
        left(uteq_rfm_ak, 4) || '-' || right(uteq_rfm_ak, 2) as Month
       ,uteq_equ_sk as NeembaEquipmentID
       ,uteq_smu_mois as OT
    from mns_utilisation_equipement
),

gold_mts_vw_tum_kpis as (
    select
        cte.mea_msi_sk as siteid,
        cte.mea_msi_code as site,
        cte.neembaequipmentid,
        cte.month,
        case when equ_hour.month is null then 0 else 1 end as hassmuformonth,
        day(last_day(cte.mea_date_debut_heure)) as daysinmonth,
        day(last_day(cte.mea_date_debut_heure)) * 24 as ct,
        sum(equ_hour.ot) as ot,
        sum(
            case 
                when cte.mea_flag_planification = 'U' 
                then datediff('second', cte.mea_date_debut_heure, cte.mea_date_fin_heure) / 3600.0
                else 0
            end
        ) as updt,
        sum(
            case 
                when cte.mea_flag_planification = 'P' 
                then datediff('second', cte.mea_date_debut_heure, cte.mea_date_fin_heure) / 3600.0
                else 0
            end
        ) as pdt,
        sum(case when cte.mea_flag_planification = 'U' then 1 else 0 end) as upc,
        sum(case when cte.mea_flag_planification = 'P' then 1 else 0 end) as pc
        -- Audit and lineage fields
        ,current_timestamp()                                              as dbt_processed_at
        -- Metadata                                   
        ,'gold_ref_vw_nba_machines'                                       as dbt_model_name
        ,'{{ var("gold_prefix") }}'                                       as layer_prefix
        ,'ref_vw_nba_machines'                                            as business_domain
    from cte
    left join equipmenthours equ_hour
           on equ_hour.neembaequipmentid = cte.neembaequipmentid
          and equ_hour.month = cte.month
    group by 
        cte.mea_msi_sk,
        cte.mea_msi_code,
        cte.neembaequipmentid,
        cte.month,
        equ_hour.month,
        day(last_day(cte.mea_date_debut_heure))

)


select * from gold_mts_vw_tum_kpis
