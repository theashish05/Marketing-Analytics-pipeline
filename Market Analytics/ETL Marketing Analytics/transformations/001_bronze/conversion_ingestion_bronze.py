from pyspark import pipelines as dp
from pyspark.sql.functions import *

VOL_LOCATION = spark.conf.get('ad_conv_loc')

@dp.table(
    name = 'ad_conversions'
)
def ad_conversions():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option('header', 'true')
        .option("multiline", "true")
        .load(VOL_LOCATION)
    )

    return df.drop('_rescued_data')