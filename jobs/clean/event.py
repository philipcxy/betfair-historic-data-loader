import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from shared.common import save_table, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    raw_df = (
        spark.table("betting.landing.raw")
        .filter(F.col("mc.marketDefinition").isNotNull())
        .select(F.col("pt"), F.col("mc.marketDefinition.*"))
    )

    window = Window.partitionBy("eventId").orderBy(F.col("pt"))

    event = raw_df.select(
        F.col("eventId").alias("id").cast(T.IntegerType()),
        F.last(F.col("eventName")).over(window).alias("name"),
        F.last(F.col("countryCode")).over(window).alias("country_code"),
        F.last(F.col("regulators")).over(window).alias("regulators"),
    ).distinct()  # Needed to remove duplicates which shouldn't exist but not digging into it right now

    save_table(spark, event, f"{namespace}.event")


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
