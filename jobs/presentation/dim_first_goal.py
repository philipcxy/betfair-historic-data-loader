import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)
    df_market = spark.read.table("soccer.market").alias("m")
    df_runner = spark.read.table("soccer.market_runner").alias("mr")

    over_under_point_five_goals_market_type_id = 63
    over_point_five_goals_runner_id = 5851483

    event = (
        df_market.filter(
            (F.col("type_id") == over_under_point_five_goals_market_type_id)
        )
        .join(df_runner, (F.col("m.id") == F.col("mr.market_id")), "inner")
        .filter((F.col("mr.winner") == F.lit(True)) & (F.col("m.kick_off").isNotNull()))
        .withColumn(
            "first_goal_minute",
            F.when(
                (F.col("mr.runner_id") == over_point_five_goals_runner_id),
                F.round(
                    (F.col("m.settled_time") - F.col("m.kick_off")).cast(T.LongType())
                    / 60,
                    0,
                ).cast(T.IntegerType()),
            ).otherwise(None),
        )
        .select(
            F.col("m.event_id"),
            F.col("first_goal_minute"),
        )
    ).alias("event_first_goal")

    save_table(spark, event, "soccer.dim_first_goal", mode=WriteMode.APPEND)


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
