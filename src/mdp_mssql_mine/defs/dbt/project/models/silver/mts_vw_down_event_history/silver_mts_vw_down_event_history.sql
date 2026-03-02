{{ 
    config(
        materialized= "view",
        tags= ["silver", "mts_vw_down_event_history"]
    )
}}


with mns_f_equipement_arret as (
    select * from {{ source('bronze', 'b_silver_mns_f_equipement_arret_v_0_1') }}
),

mns_d_type_evenement_arret as (
    select * from {{ source('bronze', 'b_silver_mns_d_type_evenement_arret') }}
),

silver_mts_vw_down_event_history as (
    select
		equ_arret.mea_sk
       ,equ_arret.mea_code
	   ,equ_arret.mea_msi_sk                                            as SiteID
	   ,equ_arret.mea_msi_code                                          as Site
       ,mea_equ_sk                                                      as NeembaEquipmentID
	   ,mea_eme_code                                                    as SerialNumber
	   ,case 
	 		when equ_arret.mea_flag_planification='U' then 'Unplanned' 
	 		when equ_arret.mea_flag_planification='P' then 'Planned' 
	 		else 'undefined' 
	    end                                                             as DownCategory
	   ,type_equ_arret.dtev_lib_type_evenement_arret                    as DownType
       ,mea_commentaire                                                 as DownComments
       ,mea_date_debut_heure                                            as Starttime
       ,mea_date_fin_heure                                              as Endtime
	   ,mea_nombre_heure*3600                                           as DurationSecs
	   ,case when equ_arret.mea_date_debut_heure is null then 1      
	         else 0      
	    end                                                             as isMissingStartTimeFlag
	   ,case when equ_arret.mea_date_debut_heure is not null      
	              and equ_arret.mea_date_fin_heure is null then 1      
	 	    else 0     
	    end                                                             as isActiveFlag
	   ,mea_annee_mois                                                  as YearMonth
        -- Audit and lineage fields
        ,current_timestamp()                                            as dbt_processed_at
        -- Metadata                                  
        ,'silver_ref_vw_nba_machines'                                   as dbt_model_name
        ,'{{ var("silver_prefix") }}'                                   as layer_prefix
        ,'ref_vw_nba_machines'                                          as business_domain
    from mns_f_equipement_arret equ_arret
         left join mns_d_type_evenement_arret type_equ_arret
                on equ_arret.mea_dtev_sk=type_equ_arret.dtev_sk

)
select * from silver_mts_vw_down_event_history
