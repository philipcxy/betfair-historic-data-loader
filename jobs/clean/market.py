import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from shared.common import get_flattened_df, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    flattened_df = get_flattened_df(spark)

    window = Window.partitionBy(F.col("fl.id"))
    market_types = spark.read.table("market_type")
    markets = (
        flattened_df.alias("fl")
        .join(market_types.alias("mt"), F.col("fl.marketType") == F.col("mt.type"))
        .select(
            F.col("fl.id"),
            F.col("eventId").alias("event_id"),
            F.col("mt.id").alias("type_id"),
            F.max("openDate").over(window).alias("scheduled_time"),
            F.max("marketTime")
            .over(window)
            .alias("start_time")
            .cast(T.TimestampType()),
            F.max("settledTime")
            .over(window)
            .alias("settled_time")
            .cast(T.TimestampType()),
            F.col("numberOfWinners").alias("num_winners"),
        )
        .distinct()
    )

    markets.write.format("iceberg").save("market")


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
