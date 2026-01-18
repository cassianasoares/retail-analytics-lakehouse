from common.spark_session import get_spark
from common.constants import (
    FILE_PRODUCTS,
    BRONZE_PRODUCTS,
    MODE_OVERWRITE,
    FORMAT_DELTA,
    INGESTION_DATE,
    HEADER_READ_FILE,
    INFER_SCHEMA_READ_FILE,
    TRUE
)

from pyspark.sql.functions import col, input_file_name, regexp_extract, current_date, coalesce
def run():
    spark = get_spark("Bronze Products")

    bronze_products = (
        spark.read
        .option(HEADER_READ_FILE, TRUE)
        .option(INFER_SCHEMA_READ_FILE, TRUE)
        .csv(FILE_PRODUCTS)
        # 1. Extraímos a data do nome do arquivo
        .withColumn(
            INGESTION_DATE, 
            regexp_extract(input_file_name(), r"ingestion_date=([^/]+)", 1)
        )
        # 2. Se a extração vier vazia ou nula, usamos a data atual (coalesce)
        # .withColumn(
        #     INGESTION_DATE,
        #     coalesce(
        #         col("extracted_date").cast("date"), 
        #         current_date()
        #     )
        # )
        # .drop("extracted_date") # Remove a coluna temporária
    )


    bronze_products.write \
        .format(FORMAT_DELTA) \
        .partitionBy(INGESTION_DATE) \
        .mode(MODE_OVERWRITE) \
        .save(BRONZE_PRODUCTS)