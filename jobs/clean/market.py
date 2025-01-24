import argparse

from pyspark.sql import SparkSession

from shared.common import save_table, setup_spark_environment


def save(namespace: str, branch: str):
    spark: SparkSession = setup_spark_environment(namespace, branch)

    markets = spark.sql("""
                        SELECT id, 
                            CAST(eventId as INT) as event_id
                            , marketType as type_id
                            , cast(max(openDate) as timestamp) as scheduled_time
                            , cast(max(marketTime) as timestamp) as start_time
                            , cast(max(settledTime) as timestamp) as settled_time
                            , min(CASE WHEN inPlay == 'true' THEN timestamp ELSE NULL END) as kick_off
                            , numberOfWinners as num_winners
                        FROM raw
                        GROUP BY id, eventId, marketType, numberOfWinners
                    """)

    save_table(spark, markets, "soccer.market")


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
