{{ 
    config(
        materialized= "view",
        tags= ["silver", "mns_utilisation_equipement"]
    )
}}


with mns_utilisation_equipement as (
    select * from {{ source('bronze', 'a_bronze_mns_f_utilisation_equipement') }}
),

silver_mns_utilisation_equipement as (
    select
       uteq_sk
      ,uteq_code
      ,uteq_equ_code as NeembaEquipmentID
      ,uteq_equ_sk
      ,uteq_mes_code
      ,uteq_mes_sk
      ,uteq_msi_code
      ,uteq_msi_sk
      ,uteq_smu_mois as OpeHours
      ,uteq_rfm_ak
    from mns_utilisation_equipement
)


select * from silver_mns_utilisation_equipement
