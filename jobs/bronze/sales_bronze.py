from common.spark_session import get_spark
from common.constants import (
    FILE_SALES,
    BRONZE_SALES,
    MODE_OVERWRITE,
    FORMAT_DELTA,
    INGESTION_DATE,
    HEADER_READ_FILE,
    INFER_SCHEMA_READ_FILE,
    TRUE
)
from pyspark.sql.functions import input_file_name, regexp_extract


def run():
    spark = get_spark("Bronze Sales")

    bronze_sales = (
        spark.read
        .option(HEADER_READ_FILE, TRUE)
        .option(INFER_SCHEMA_READ_FILE, TRUE)
        .csv(FILE_SALES)
        .withColumn(
            INGESTION_DATE,
            regexp_extract(input_file_name(), r"ingestion_date=([^/]+)", 1)
        )
    )

    bronze_sales.write \
        .format(FORMAT_DELTA) \
        .partitionBy(INGESTION_DATE) \
        .mode(MODE_OVERWRITE) \
        .save(BRONZE_SALES)