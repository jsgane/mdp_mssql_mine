{{ 
    config(
        materialized= "view",
        tags= ["silver", "mns_site"]
    )
}}


with mns_site as (
    select * from {{ source('bronze', 'a_bronze_mns_d_site') }}
),

silver_mns_site as (
    select distinct
        msi_sk                        as MTS_SiteID
       ,msi_code                      as SiteCode
       ,msi_lib_site                  as SiteDesc
       ,msi_latitude                  as Latitude
       ,msi_longitude                 as Longitude

        -- Audit and lineage fields
        ,current_timestamp()          as dbt_processed_at
        -- Metadata
        ,'silver_mns_site'            as dbt_model_name
        ,'{{ var("silver_prefix") }}' as layer_prefix
        ,'mns_site'                   as business_domain
    from mns_site 
    where msi_sk > 1
      and msi_code IN (
                    'SIG',
                    'FEK',
                    'KRSC',
                    'SEG',
                    'AGB',
                    'SIMM',
                    'FDE',
                    'T014',
                    'BKR',
                    'KIAK',
                    'BOG',
                    'RSSA',
                    'MHDT',
                    'PG11',
                    'ESK',
                    'CBG',
                    'SAB',
                    'TON',
                    'SAD'
                )
)
select * from silver_mns_site
