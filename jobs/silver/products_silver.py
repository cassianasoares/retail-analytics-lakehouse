from common.spark_session import get_spark
from common.constants import (
    BRONZE_PRODUCTS,
    SILVER_PRODUCTS,
    MODE_OVERWRITE,
    FORMAT_DELTA,
    INGESTION_DATE,
    PRODUCT_ID
)
from pyspark.sql.functions import col,row_number
from pyspark.sql.window import Window

ROW_NUMBER = "rn"

def run():
    window = Window.partitionBy(PRODUCT_ID).orderBy(col(INGESTION_DATE).desc())
    spark = get_spark("Silver Products")

    bronze_products_delta = (
        spark.read
        .format(FORMAT_DELTA)
        .load(BRONZE_PRODUCTS)
    )

    silver_products = (
        bronze_products_delta
        .withColumn(ROW_NUMBER, row_number().over(window))
        .filter("rn = 1")
        .drop(ROW_NUMBER)
    )

    silver_products.write \
        .format(FORMAT_DELTA) \
        .mode(MODE_OVERWRITE) \
        .option("overwriteSchema", "true") \
        .save(SILVER_PRODUCTS)