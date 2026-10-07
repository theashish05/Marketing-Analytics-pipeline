from pyspark import pipelines as dp
from pyspark.sql.functions import *

VOL_LOCATION = spark.conf.get('ad_perf_loc')

SCHEMA = """
    campaign_id STRING,
    date DATE,
    impressions LONG,
    clicks LONG,
    spend_usd DOUBLE,
    device STRING
"""

@dp.table(
    name = 'ad_performance'
)
def ad_performance():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option('header', 'true')
        .schema(SCHEMA)
        .load(VOL_LOCATION)
    )

    return df.drop('_rescued_data')