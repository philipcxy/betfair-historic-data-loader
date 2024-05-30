import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

from shared.common import setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    runner = spark.read.table("runner").alias("r")
    runner_change = spark.read.table("runner_change").alias("rc")
    market_runner = spark.read.table("market_runner").alias("mr")
    market = spark.read.table("market").alias("m")
    market_type = spark.read.table("market_type").alias("mt")
    event = spark.read.table("event").alias("e")

    window = Window.partitionBy(F.col("rc.market_id"), F.col("rc.runner_id"))
    runner_change_pre_ko_df = (
        runner_change.join(
            F.broadcast(market), F.col("m.id") == F.col("rc.market_id"), how="inner"
        )
        .filter(F.col("rc.timestamp") < F.col("kick_off"))
        .withColumn("total_traded_volume", F.sum(F.col("rc.tv")).over(window))
        .select(
            F.col("market_id"),
            F.col("runner_id"),
            F.sum(F.col("rc.tv")).over(window).alias("pre_ko_traded_volume"),
        )
        .distinct()
    ).alias("rc_pko")

    runner_change = runner_change.select(
        F.col("market_id"),
        F.col("runner_id"),
        F.sum(F.col("rc.tv")).over(window).alias("total_traded_volume"),
    ).distinct()

    dim_runner = (
        runner.join(market_runner, F.col("r.id") == F.col("mr.runner_id"))
        .join(market, F.col("mr.market_id") == F.col("m.id"))
        .join(market_type, F.col("m.type_id") == F.col("mt.id"))
        .join(event, F.col("m.event_id") == F.col("e.id"))
        .join(
            runner_change,
            (F.col("mr.market_id") == F.col("rc.market_id"))
            & (F.col("mr.runner_id") == F.col("rc.runner_id")),
        )
        .join(
            runner_change_pre_ko_df,
            (F.col("mr.market_id") == F.col("rc_pko.market_id"))
            & (F.col("mr.runner_id") == F.col("rc_pko.runner_id")),
        )
    )

    dim_runner = (
        (
            dim_runner.withColumn(
                "scheduled_date_key",
                F.year(F.col("m.scheduled_time")) * 10000
                + F.month(F.col("m.scheduled_time")) * 100
                + F.dayofmonth(F.col("m.scheduled_time")),
            )
            .withColumn(
                "scheduled_time_key",
                F.date_format(F.col("m.scheduled_time"), "HHmm").cast(T.IntegerType()),
            )
            .withColumn(
                "kick_off_date_key",
                F.year(F.col("m.kick_off")) * 10000
                + F.month(F.col("m.kick_off")) * 100
                + F.dayofmonth(F.col("m.kick_off")),
            )
            .withColumn(
                "kick_off_time_key",
                F.date_format(F.col("m.kick_off"), "HHmm").cast(T.IntegerType()),
            )
            .select(
                F.monotonically_increasing_id().alias("id"),
                F.col("r.id").alias("runner_id"),
                F.col("r.name"),
                F.col("m.id").alias("market_id"),
                F.col("mt.id").alias("market_type_id"),
                F.col("mt.type").alias("market_name"),
                F.col("e.id").alias("event_id"),
                F.col("e.name").alias("event_name"),
                F.col("scheduled_date_key"),
                F.col("scheduled_time_key"),
                F.col("kick_off_date_key"),
                F.col("kick_off_time_key"),
                F.col("e.country_code"),
                F.col("rc_pko.pre_ko_traded_volume"),
                F.col("total_traded_volume"),
            )
            .distinct()
        )
        .write.format("iceberg")
        .save("dim_runner")
    )


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
