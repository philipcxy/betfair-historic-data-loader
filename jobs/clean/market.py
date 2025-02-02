import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    raw_df = (
        spark.table("betting.landing.raw")
        .alias("r")
        .filter(F.col("mc.marketDefinition").isNotNull())
        .select(F.col("pt"), F.col("mc.marketDefinition.*"))
    )
    market_type_df = spark.table("betting.clean.market_type").alias("mt")

    markets = (
        raw_df.join(
            market_type_df, (F.col("r.marketType") == F.col("mt.type")), "inner"
        )
        .select(
            F.col("id"),
            F.col("eventId").alias("event_id").cast(T.IntegerType()),
            F.col("mt.id").alias("type_id"),
            F.max(F.col("openDate")).cast("timestamp").alias("scheduled_time"),
            F.max(F.col("marketTime")).cast("timestamp").alias("start_time"),
            F.max(F.col("settledTime")).cast("timestamp").alias("settled_time"),
            F.min(
                F.when(F.col("inPlay") == "true", F.col("timestamp")).otherwise(None)
            ).alias("kick_off"),
            F.col("numberOfWinners").alias("num_winners"),
        )
        .groupBy(F.col("id"), F.col("event_id"), F.col("type_id"), F.col("num_winners"))
    )

    save_table(spark, markets, f"{namespace}.market", WriteMode.APPEND)


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
