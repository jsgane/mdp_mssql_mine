"""
DBT Asset Sensors.

Auto-trigger dbt assets when their dependencies are ready.
"""

from mdp_common.dbt import create_multi_asset_sensor


# Bronze → Silver work order headers and lines
silver_quote_work_order_sensor = create_multi_asset_sensor(
    sensor_name="silver_quote_work_order_sensor",
    monitored_asset_names=["silver_quote_work_order"],
    job_name="refresh_silver_quote_work_order",
    minimum_interval_seconds=30,
)

silver_customized_field_sensor = create_multi_asset_sensor(
    sensor_name="silver_customized_field_sensor",
    monitored_asset_names=["silver_customized_field"],
    job_name="refresh_silver_customized_field",
    minimum_interval_seconds=30,
)

silver_order_service_sensor = create_multi_asset_sensor(
    sensor_name="silver_order_service",
    monitored_asset_names=["silver_order_service"],
    job_name="refresh_silver_order_service",
    minimum_interval_seconds=30,
)

silver_delivery_service_sensor = create_multi_asset_sensor(
    sensor_name="silver_delivery_service",
    monitored_asset_names=["silver_delivery_service"],
    job_name="refresh_silver_delivery_service",
    minimum_interval_seconds=30,
)

silver_invoice_service_sensor = create_multi_asset_sensor(
    sensor_name="silver_invoice_service",
    monitored_asset_names=["service_invoice_header", "service_cession_invoice_header", "service_invoice_footer", "service_repair_order_line"],
    job_name="refresh_silver_invoice_service",
    minimum_interval_seconds=30,
)
####
silver_component_code_sensor = create_multi_asset_sensor(
    sensor_name="silver_component_code,
    monitored_asset_names=["reference_codes"],
    job_name="refresh_silver_component_code",
    minimum_interval_seconds=30,
)

silver_location_code_sensor = create_multi_asset_sensor(
    sensor_name="silver_location_code,
    monitored_asset_names=["reference_codes"],
    job_name="refresh_silver_location_code",
    minimum_interval_seconds=30,
)

silver_hour_type_sensor = create_multi_asset_sensor(
    sensor_name="silver_hour_type,
    monitored_asset_names=["reference_codes"],
    job_name="refresh_silver_hour_type",
    minimum_interval_seconds=30,
)

silver_intervention_type_sensor = create_multi_asset_sensor(
    sensor_name="silver_intervention_type,
    monitored_asset_names=["reference_codes"],
    job_name="refresh_silver_intervention_type",
    minimum_interval_seconds=30,
)

silver_stock_contract_sensor = create_multi_asset_sensor(
    sensor_name="silver_stock_contract,
    monitored_asset_names=["reference_codes"],
    job_name="refresh_silver_stock_contract",
    minimum_interval_seconds=30,
)

silver_ro_type_sensor = create_multi_asset_sensor(
    sensor_name="silver_ro_type,
    monitored_asset_names=["reference_codes"],
    job_name="refresh_silver_ro_type",
    minimum_interval_seconds=30,
)

######### GOLD ##########
gold_quote_work_order_sensor = create_multi_asset_sensor(
    sensor_name="gold_quote_work_order_sensor",
    monitored_asset_names=["silver_quote_work_order"],
    job_name="refresh_gold_quote_work_order",
    minimum_interval_seconds=30,
)



