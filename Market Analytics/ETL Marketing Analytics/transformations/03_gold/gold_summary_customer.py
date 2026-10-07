from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name = '03_gold.mv_gold_customer_summary'
)
def mv_gold_customer_summary():
    df_conv = (
    spark.read.table('marketing_analytics_project.`02_silver`.fact_ad_conversion')
    .groupBy('customer_id')
    .agg(
        sum('revenue_usd').alias('revenue_usd'),
        sum('add_to_cart').alias('add_to_cart'),
        sum('wishlist').alias('wishlist'),
        sum('purchase').alias('purchase'),
        sum(col('add_to_cart') + col('wishlist') + col('purchase')).alias('total_no_of_conv')
    )
    .withColumn('revenue_usd', round(col('revenue_usd'), 2))
    )

    df_cust = spark.read.table('marketing_analytics_project.`02_silver`.dim_customer')

    df_join = (
        df_conv
        .join(df_cust, 'customer_id', 'left')
        .withColumn('customer_tier', expr("CASE WHEN revenue_usd < 100 OR total_no_of_conv < 10 THEN 'Bronze' WHEN revenue_usd >= 500 OR total_no_of_conv >= 15 THEN 'Gold' ELSE 'Silver' END"))
        .select('customer_id', 'region', 'customer_tier', 'revenue_usd', "add_to_cart", "wishlist", "purchase", "total_no_of_conv")
        .orderBy('customer_id')
    )

    return df_join