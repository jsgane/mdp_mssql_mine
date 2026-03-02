{{ 
    config(
        materialized= "view",
        tags= ["silver", "ref_vw_nba_machines"]
    )
}}


with ref_vw_nba_machines as (
    select * from {{ source('bronze', 'a_bronze_ref_vw_nba_machines') }}
),
RankedData as (
    select
         MineStarOID
        ,NeembaEquipmentID
        ,SiteCode
        ,SiteDesc
        ,SiteID
        ,Category
        ,Model
        ,SerialNumber
        ,FleetName
        ,MineStarSerialNumber
        ,Manufacturer
        ,NominalPayload
        ,EquipmentStatus
		,case when NominalPayload is null then 'No' else 'Yes' end as payloadAvailable
        ,ROW_NUMBER() OVER (partition by NeembaEquipmentID order by SiteID) as RowNum
    from ref_vw_nba_machines    
    where NeembaEquipmentID != -2
),
silver_ref_vw_nba_machines as (
    select 
         MineStarOID
        ,NeembaEquipmentID
        ,SiteCode
        ,SiteDesc
        ,SiteID
        ,Category
        ,Model
        ,SerialNumber
        ,FleetName
        ,MineStarSerialNumber
        ,Manufacturer
        ,NominalPayload
        ,EquipmentStatus
    	,payloadAvailable
        -- Audit and lineage fields
        ,current_timestamp()          as dbt_processed_at
        -- Metadata
        ,'silver_ref_vw_nba_machines' as dbt_model_name
        ,'{{ var("silver_prefix") }}' as layer_prefix
        ,'ref_vw_nba_machines'        as business_domain
    from RankedData
    where RowNum = 1
      and SiteCode in (
        'SIG',
        'FEK',
        'KRSC',
        'SEG',
        'AGB',
        'SIMM',
        'FDE',
        'T014',
        'TIND',
        'BKR',
        'KIAK',
        'BOG',
        'RSSA',
        'MHDT',
        'PG11',
        'ESK',
        'CBG',
        'SAB',
        'TON'
      )
)
select * from silver_ref_vw_nba_machines
