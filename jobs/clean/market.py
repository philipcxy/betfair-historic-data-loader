import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    raw_df = (
        spark.table("betting.landing.raw")
        .filter(F.col("mc.marketDefinition").isNotNull())
        .select(F.col("timestamp"), F.col("mc.id"), F.col("mc.marketDefinition.*"))
    ).alias("r")

    market_types = spark.read.table("betting.clean.market_type")

    window = (
        Window.partitionBy(F.col("r.id"))
        .orderBy(F.col("r.timestamp"))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    markets = (
        raw_df.join(market_types.alias("mt"), F.col("r.marketType") == F.col("mt.type"))
        .withColumn(
            "only_inplay",
            F.when(F.col("inPlay") == F.lit(True), F.col("r.timestamp")).otherwise(
                F.lit(None)
            ),
        )
        .select(
            F.col("r.id"),
            F.col("eventId").alias("event_id").cast(T.IntegerType()),
            F.col("mt.id").alias("type_id"),
            F.max("openDate")
            .over(window)
            .alias("scheduled_time")
            .cast(T.TimestampType()),
            F.max("marketTime")
            .over(window)
            .alias("start_time")
            .cast(T.TimestampType()),
            F.max("settledTime")
            .over(window)
            .alias("settled_time")
            .cast(T.TimestampType()),
            F.first(F.col("only_inplay"), ignorenulls=True)
            .over(window)
            .alias("kick_off"),
            F.col("numberOfWinners").alias("num_winners"),
        )
        .distinct()
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
