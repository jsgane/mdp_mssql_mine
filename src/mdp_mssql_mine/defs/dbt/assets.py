from dagster import AssetExecutionContext, Output, MetadataValue
from typing import Dict, Any
from pathlib import Path

from mdp_common.dbt import execute_dbt, create_dbt_asset
from ..config import GROUP_SILVER, GROUP_GOLD

#### Silver

dbt_silver_quote_work_order= create_dbt_asset(
    asset_name="silver_quote_work_order",
    dbt_model_name="silver_quote_work_order",
    dbt_project_name="mdp_service",
    schema_name="services",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["service_repair_order_header","service_repair_order_line", "service_intervention", "customized_field"],
    metadata_queries={
        "total_entetes":"SELECT count(*) FROM services.B_SILVER_quote_work_order",

    },
    description="",
    dbt_project_dir=Path(__file__).parent /"project",

)

dbt_silver_customized_field= create_dbt_asset(
    asset_name="silver_customized_field",
    dbt_model_name="silver_customized_field",
    dbt_project_name="mdp_service",
    schema_name="services",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["customized_field"],
    metadata_queries={
        "total_entetes":"SELECT count(*) FROM services.B_SILVER_customized_field",

    },
    description="",
    dbt_project_dir=Path(__file__).parent /"project",

)


dbt_silver_order_service = create_dbt_asset(
    asset_name="silver_order_service",
    dbt_model_name="silver_order_service",
    dbt_project_name="mdp_service",
    schema_name="services",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["service_repair_order_header","service_repair_order_line", "service_intervention", "customized_field"],
    metadata_queries={
        "total_entetes":"SELECT count(*) FROM services.B_SILVER_order_service",

    },
    description="",
    dbt_project_dir=Path(__file__).parent /"project",

)



