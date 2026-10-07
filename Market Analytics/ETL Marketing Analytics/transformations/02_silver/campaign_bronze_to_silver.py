from pyspark import pipelines as dp

# Create the target streaming table in the silver schema
dp.create_streaming_table(
    name="marketing_analytics_project.`02_silver`.dim_campaign"
)

# SCD Type 1: keeps only the latest record per campaign_id
dp.create_auto_cdc_flow(
    target="marketing_analytics_project.`02_silver`.dim_campaign",
    source="marketing_analytics_project.`01_bronze`.ad_campaign",
    keys=["campaign_id"],
    sequence_by="start_date",
    stored_as_scd_type=1
)