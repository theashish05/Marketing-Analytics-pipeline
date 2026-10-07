from pyspark import pipelines as dp
from pyspark.sql.functions import *

VOL_LOCATION = spark.conf.get('ad_cust_loc')

SCHEMA = """
    customer_id STRING,
    signup_date DATE,
    region STRING,
    segment STRING
"""


@dp.table(
    name = 'ad_customer'
)
def ad_customer():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option('header', 'true')
        .schema(SCHEMA)
        .load(VOL_LOCATION)
    )

    return df.drop('_rescued_data')