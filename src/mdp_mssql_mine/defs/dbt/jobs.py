from mdp_common.dbt import create_dbt_job


refresh_silver_quote_work_order = create_dbt_job(
    asset_name="silver_quote_work_order",
    job_name="refresh_silver_quote_work_order",
    description="Refresh silver quote work order headers and lines",
)

refresh_silver_customized_field = create_dbt_job(
    asset_name="silver_customized_field",
    job_name="refresh_silver_customized_field",
    description="Refresh silver customized fields",
)

refresh_silver_order_service = create_dbt_job(
    asset_name="silver_order_service",
    job_name="refresh_silver_order_service",
    description="Refresh silver order service",
)

refresh_silver_delivery_service = create_dbt_job(
    asset_name="silver_delivery_service",
    job_name="refresh_silver_delivery_service",
    description="Refresh silver delivery service",
)

refresh_silver_invoice_service = create_dbt_job(
    asset_name="silver_invoice_service",
    job_name="refresh_silver_invoice_service",
    description="Refresh silver invoice service",
)
##

refresh_silver_component_code = create_dbt_job(
    asset_name="silver_component_code",
    job_name="refresh_silver_component_code",
    description="Refresh silver component_code",
)

refresh_silver_location_code = create_dbt_job(
    asset_name="silver_location_code",
    job_name="refresh_silver_location_code",
    description="Refresh silver location_code",
)

refresh_silver_hour_type = create_dbt_job(
    asset_name="silver_hour_type",
    job_name="refresh_silver_hour_type",
    description="Refresh silver hour_type",
)

refresh_silver_intervention_type = create_dbt_job(
    asset_name="silver_intervention_type",
    job_name="refresh_silver_intervention_type",
    description="Refresh silver intervention_type",
)

refresh_silver_stock_contract = create_dbt_job(
    asset_name="silver_stock_contract",
    job_name="refresh_silver_stock_contract",
    description="Refresh silver stock_contract",
)

refresh_silver_ro_type = create_dbt_job(
    asset_name="silver_ro_type",
    job_name="refresh_silver_ro_type",
    description="Refresh silver ro_type",
)


########## GOLD ############
refresh_gold_quote_work_order = create_dbt_job(
    asset_name="gold_quote_work_order",
    job_name="refresh_gold_quote_work_order",
    description="Refresh gold quote work order headers and lines",
)




