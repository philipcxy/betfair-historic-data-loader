import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.common import get_flattened_df, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    flattened_df = get_flattened_df(spark)

    event = (
        flattened_df.withColumn("regulators", F.element_at(0, F.col("regulators")))
        .select(
            F.col("eventId").alias("id").cast(T.IntegerType()),
            F.col("eventName").alias("name"),
            F.col("countryCode").alias("country_code"),
            F.col("regulators"),
        )
        .distinct()
    )

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
