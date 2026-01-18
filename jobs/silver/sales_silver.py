from common.spark_session import get_spark
from common.constants import (
    BRONZE_SALES,
    SILVER_SALES,
    MODE_OVERWRITE,
    FORMAT_DELTA,
    ORDER_DATE,
    YEAR,
    MONTH,
    DAY
)
from pyspark.sql.functions import col,to_date, year, month, dayofmonth

def run():
    spark = get_spark("Silver Sales")

    silver_sales = (
        spark.read
        .format(FORMAT_DELTA)
        .load(BRONZE_SALES)
        .filter("quantity > 0")
        .withColumn(ORDER_DATE, to_date(col(ORDER_DATE)))
        .withColumn(YEAR, year(ORDER_DATE))
        .withColumn(MONTH, month(ORDER_DATE))
        .withColumn(DAY, dayofmonth(ORDER_DATE))
    )

    silver_sales.write \
    .format(FORMAT_DELTA) \
    .partitionBy(YEAR, MONTH, DAY) \
    .mode(MODE_OVERWRITE) \
    .save(SILVER_SALES)