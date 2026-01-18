from common.spark_session import get_spark
from pyspark.sql import DataFrame
from common.constants import (
    GOLD_CROSS_SELL,
    SILVER_SALES,
    MODE_OVERWRITE,
    FORMAT_DELTA,
    PRODUCT_ID,
    PRODUCT_Y_ID
)
from pyspark.sql.functions import col,lit, collect_set, dayofweek
from pyspark.ml.fpm import FPGrowth
from functools import reduce

ITEMS = "items"
CAMPAINGN_DAY = "campaign_day"

def run():
    spark = get_spark("Gold Cross Sell")

    silver_delta = (
        spark.read
        .format(FORMAT_DELTA)
        .load(SILVER_SALES)
    )

    transactions_by_day = (
        silver_delta
        .withColumn(CAMPAINGN_DAY, dayofweek("order_date"))  # 1=Domingo ... 7=Sábado
        .groupBy(CAMPAINGN_DAY, "order_id")
        .agg(collect_set(PRODUCT_ID).alias(ITEMS))
    )

    def run_fp_growth_by_day(df: DataFrame):
        days = [row.campaign_day for row in df.select(CAMPAINGN_DAY).distinct().collect()]
        results = []

        for day in days:
            daily_tx = df.filter(col(CAMPAINGN_DAY) == day)

            fp = FPGrowth(
                itemsCol=ITEMS,
                minSupport=0.01,
                minConfidence=0.3
            )

            model = fp.fit(daily_tx)

            rules = (
                model.associationRules
                .withColumn(CAMPAINGN_DAY, lit(day))
            )

            results.append(rules)

        return results


    rules_by_day = run_fp_growth_by_day(transactions_by_day)

    all_rules = reduce(
        DataFrame.unionByName,
        rules_by_day
    )

    
    gold_cross_sell = (
        all_rules
        .filter(col("lift") > 1.2)
        .filter(col("confidence") > 0.4)
        .withColumn("product_x_id", col("antecedent")[0])
        .withColumn(PRODUCT_Y_ID, col("consequent")[0])
        .withColumn("action_type", lit("CROSS_SELL"))
        .select(
            CAMPAINGN_DAY,
            "product_x_id",
            PRODUCT_Y_ID,
            "action_type",
            "confidence",
            "lift",
            "support"
        )
    )


    gold_cross_sell.write \
        .format(FORMAT_DELTA) \
        .mode(MODE_OVERWRITE) \
        .save(GOLD_CROSS_SELL)