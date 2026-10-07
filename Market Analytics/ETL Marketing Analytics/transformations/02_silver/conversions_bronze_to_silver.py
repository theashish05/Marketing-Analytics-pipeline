from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table(name="02_silver.fact_ad_conversion")
def fact_ad_conversion():
    df = (
        spark.read.table("marketing_analytics_project.`01_bronze`.ad_conversions")
        .withColumn("date", to_date(col("date")))
        .withColumn(
            "conversion",
            from_json(
                col("conversion"),
                "ARRAY<STRUCT<customer_id:STRING,conversion_type:STRING,revenue_usd:DOUBLE>>",
            ),
        )
        .withColumn("conversion", explode(col("conversion")))
        .withColumn("revenue_usd", col("conversion.revenue_usd"))
        .withColumn("conversion_type", col("conversion.conversion_type"))
        .withColumn("customer_id", col("conversion.customer_id"))
        .drop("conversion")
        .select("campaign_id", "date", "revenue_usd", "conversion_type", "customer_id")
    )

    df_transpose = (
        df.withColumn(
            "add_to_cart", when(col("conversion_type") == "Add to Cart", 1).otherwise(0)
        )
        .withColumn(
            "wishlist", when(col("conversion_type") == "Wishlist", 1).otherwise(0)
        )
        .withColumn(
            "purchase", when(col("conversion_type") == "Purchase", 1).otherwise(0)
        )
        .groupBy("campaign_id", "date", "customer_id")
        .agg(
            sum("revenue_usd").alias("revenue_usd"),
            sum("add_to_cart").alias("add_to_cart"),
            sum("wishlist").alias("wishlist"),
            sum("purchase").alias("purchase"),
        )
        .orderBy("date")
    )
    return df_transpose
