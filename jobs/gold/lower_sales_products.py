from common.spark_session import get_spark
from common.constants import (
    GOLD_LOW_SALES,
    SILVER_SALES,
    MODE_OVERWRITE,
    FORMAT_DELTA,
    QUANTITY,
    TOTAL_QUANTITY,
    PRODUCT_ID,
    VALUE_TO_DEFINE_LOW_SALES
)
from pyspark.sql.functions import col, sum

def run():
    spark = get_spark("Gold Lower Sales")

    silver_sales = (
        spark.read
        .format(FORMAT_DELTA)
        .load(SILVER_SALES)
    )


    low_sales_products = (
        silver_sales
        .groupBy(PRODUCT_ID)
        .agg(sum(QUANTITY).alias(TOTAL_QUANTITY))
        .filter(col(TOTAL_QUANTITY) < VALUE_TO_DEFINE_LOW_SALES)
    )

    low_sales_products.write \
        .format(FORMAT_DELTA)\
        .mode(MODE_OVERWRITE)\
        .save(GOLD_LOW_SALES)