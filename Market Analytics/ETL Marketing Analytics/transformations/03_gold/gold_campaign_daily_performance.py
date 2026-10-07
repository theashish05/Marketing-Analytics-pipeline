from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name = '03_gold.mv_gold_campaign_daily_performance'
)
def mv_gold_campaign_daily_performance():
    df_conv = (
    spark.read.table('marketing_analytics_project.`02_silver`.fact_ad_conversion')
    .groupBy('campaign_id', 'date')
    .agg(
        sum('revenue_usd').alias('revenue_usd'),
        sum('add_to_cart').alias('add_to_cart'),
        sum('wishlist').alias('wishlist'),
        sum('purchase').alias('purchase'),
        sum(col('add_to_cart') + col('wishlist') + col('purchase')).alias('total_no_of_conv')
    )
    .withColumn('revenue_usd', round(col('revenue_usd'), 2))
    )

    df_ad_perf = (
        spark.read.table('marketing_analytics_project.`02_silver`.fact_ad_performance')
        .groupBy('campaign_id', 'date')
        .agg(
            sum('impressions').alias('impressions'),
            sum('clicks').alias('clicks'),
            sum('spend_usd').alias('spend_usd')
        )
        .withColumn('spend_usd', round(col('spend_usd'), 2))
    )

    df_join = (
        df_ad_perf
        .join(df_conv, ['campaign_id', 'date'], 'full_outer')
        .fillna(0)
        .fillna('NA')
    )

    df_camp = spark.read.table('marketing_analytics_project.`02_silver`.dim_campaign')

    df_final = (
        df_join
        .join(df_camp, 'campaign_id', 'left')
        .select("date", "campaign_id", "campaign_name", "channel", "impressions", "clicks", "spend_usd", "revenue_usd", "add_to_cart", "wishlist", "purchase", "total_no_of_conv")
        .orderBy('date', 'campaign_id')
    )

    return df_final