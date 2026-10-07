from pyspark import pipelines as dp
from pyspark.sql.functions import *

QualityCheck = {
    "valid_clicks": "clicks < impressions",
    "valid_spend": "spend_usd >= 0.00"
}

@dp.table(
    name = '02_silver.fact_ad_performance'
)
@dp.expect_all_or_drop(QualityCheck)

def fact_ad_performance():
    df = spark.readStream.table("marketing_analytics_project.`01_bronze`.ad_performance")
    df_transform=(
        df.dropDuplicates(["campaign_id","date","device"])
          .fillna({
              'impressions':0,
              'clicks':0,
              'spend_usd':0
          })                
           .where('impressions < 0')
           .withColumn('spend_usd',col('spend_usd')/lit(95))               
                          
    )
    return df_transform