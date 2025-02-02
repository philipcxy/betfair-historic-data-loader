import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.common import save_table, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    raw_df = (
        spark.table("betting.landing.raw")
        .filter(F.col("mc.marketDefinition").isNotNull())
        .select(
            F.col("mc.id").alias("market_id"),
            F.col("pt").alias("epoch"),
            F.col("timestamp"),
            F.col("mc.marketDefinition.*"),
        )
    )

    market_change = raw_df.select(
        F.col("market_id"),
        F.col("epoch"),
        F.col("timestamp"),
        F.col("betDelay").alias("bet_delay"),
        F.col("bettingType").alias("betting_type"),
        F.col("bspMarket").alias("bsp_market"),
        F.col("bspReconciled").alias("bsp_reconciled"),
        F.col("complete"),
        F.col("crossMatching").alias("cross_matching"),
        F.col("discountAllowed").alias("discount_allowed"),
        F.col("inPlay").alias("in_play"),
        F.col("marketBaseRate").alias("base_rate"),
        F.col("openDate").alias("open_date"),
        F.col("persistenceEnabled").alias("persistence_enabled"),
        F.col("runnersVoidable").alias("runners_voidable"),
        F.col("status"),
        F.col("version"),
    )

    save_table(spark, market_change, f"{namespace}.market_change")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a PySpark job")
    parser.add_argument(
        "--namespace",
        type=str,
        required=False,
        dest="namespace",
        help="If specified creates a new namespace and uses it",
    )
    parser.add_argument(
        "--branch",
        type=str,
        required=False,
        dest="branch",
        help="If specified creates a new branch in nessie and uses it",
    )
    args = parser.parse_args()

    save(args.namespace, args.branch)
