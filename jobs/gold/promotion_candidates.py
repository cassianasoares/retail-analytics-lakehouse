from common.spark_session import get_spark
from common.constants import (
    SILVER_PRODUCTS,
    GOLD_CROSS_SELL,
    GOLD_LOW_SALES,
    GOLD_PROMOTION_CANDIDATES,
    MODE_OVERWRITE,
    FORMAT_DELTA,
    PRODUCT_ID
)
from pyspark.sql.functions import col,row_number

PRODUCT_Y_ID = "product_y_id"
CATEGORY = "category"
INNER = "inner"

def run():
    spark = get_spark("Gold Promotion Candidates")

    silver_products = (
        spark.read
        .format(FORMAT_DELTA)
        .load(SILVER_PRODUCTS)
    )

    gold_cross_sell = (
        spark.read
        .format(FORMAT_DELTA)
        .load(GOLD_CROSS_SELL)
    )

    low_sales_products = (
        spark.read
        .format(FORMAT_DELTA)
        .load(GOLD_LOW_SALES)
    )

    promotion_candidates = (
        gold_cross_sell
        .join(
            silver_products.select(
                col(PRODUCT_ID).alias(PRODUCT_Y_ID),
                col(CATEGORY)
            ),
            on=PRODUCT_Y_ID,
            how=INNER
        )
        .join(
            low_sales_products.select(PRODUCT_ID),
            low_sales_products.product_id == col(PRODUCT_Y_ID),
            how=INNER
        )
        .select(
            col("campaign_day"),
            col("product_x_id"),
            col(PRODUCT_Y_ID),
            col(CATEGORY).alias("target_category"),
            col("action_type"),
            col("confidence"),
            col("lift"),
            col("support")
        )
    )

    promotion_candidates.write \
        .format(FORMAT_DELTA) \
        .mode(MODE_OVERWRITE) \
        .save(GOLD_PROMOTION_CANDIDATES)