import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

from shared.common import save_table, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    raw_df = (
        spark.table("betting_warehouse.landing.raw")
            .filter(F.col("mc.marketDefinition").isNotNull())
            .select(
                F.col("pt"),
                F.col("mc.id"),
                F.col("source_file_names"),
                F.col("mc"),
                F.col("mc.marketDefinition.*"),
            )
    ).alias("r")

    window = (
        Window.partitionBy(F.col("r.id"))
        .orderBy(F.col("r.pt"))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    markets = (
        raw_df.withColumn(
            "only_inplay",
            F.when(F.col("inPlay") == F.lit(True), F.col("r.pt")).otherwise(F.lit(None)),
        ).select(
            F.last("r.id").over(window).alias("market_id"),
            F.last("r.name").over(window).alias("market_name"),
            F.last("r.marketType").over(window).alias("market_type"),
            F.last("eventId").over(window).alias("event_id").cast(T.IntegerType()),
            F.last("eventName").over(window).alias("event_name"),
            F.last("countryCode").over(window).alias("country_code"),
            F.max("openDate").over(window).alias("scheduled_time").cast(T.TimestampType()),
            F.max("marketTime").over(window).alias("start_time").cast(T.TimestampType()),
            F.max("settledTime").over(window).alias("settled_time").cast(T.TimestampType()),
            F.to_timestamp(
                F.first(F.col("only_inplay"), ignorenulls=True).over(window) / 1000
            ).alias("kick_off"),
            F.last("numberOfWinners").over(window).alias("num_winners"),
            F.last("source_file_names").over(window).alias("source_file_names"),
        )
    ).distinct()
    
    save_table(spark, markets, "betting_warehouse.clean.market")


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
