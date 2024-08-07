import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from shared.common import get_flattened_df, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    flattened_df = get_flattened_df(spark)

    window = Window.partitionBy("eventId").orderBy(F.desc(F.col("pt")))

    event = flattened_df.select(
        F.col("eventId").alias("id").cast(T.IntegerType()),
        F.first(F.col("eventName")).over(window).alias("name"),
        F.first(F.col("countryCode")).over(window).alias("country_code"),
        F.first(F.col("regulators")).over(window).alias("regulators"),
    ).distinct()

    event.write.format("iceberg").mode("append").save("event")


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
