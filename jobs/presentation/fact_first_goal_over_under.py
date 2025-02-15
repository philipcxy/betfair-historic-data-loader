import argparse

from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.common import save_table, setup_spark_environment
from shared.enums import WriteMode


def load_data_to_table(namespace: str, branch: str) -> None:
    spark = setup_spark_environment(namespace, branch)

    df = spark.sql("""
        SELECT r.event_id,
            r.event_name,
            r.market_id, 
            r.market_name,
            r.id AS runner_key,
            r.runner_id,
            r.name,
            CAST(from_unixtime(first(fe.epoch/1000)) AS date) as timestamp,
            r.pre_ko_traded_volume,
            r.ko_odds,
            r.favourite,
            r.sort_priority,
            r.winner,
            r.country_code,
            percentile_approx(fe.first_odds, 0.5) as first_odds,
            percentile_approx(fe.second_odds, 0.5) as second_odds,
            percentile_approx(fe.third_odds, 0.5) as third_odds,
            e.first_goal_minute
        FROM betting.presentation.dim_runner r
        INNER JOIN betting.clean.market_type mt
            ON r.market_type_id = mt.id
        INNER JOIN betting.presentation.fact_runner_odds fe
            ON r.id = fe.runner_key
        INNER JOIN betting.presentation.dim_first_goal e
            ON  r.event_id = e.event_id
        WHERE (mt.type = "OVER_UNDER_15" or mt.type = "OVER_UNDER_25") 
            AND fe.minute between e.first_goal_minute + 2 AND e.first_goal_minute + 5
        GROUP BY r.event_id,
            r.event_name,
            r.market_id, 
            r.market_name,
            r.id,
            r.runner_id,
            r.name,
            r.pre_ko_traded_volume, 
            r.ko_odds, 
            r.favourite, 
            r.sort_priority, 
            r.winner, 
            r.country_code, 
            e.first_goal_minute
    """)

    save_table(
        spark, df, f"{namespace}.fact_first_goal_match_odds", mode=WriteMode.REPLACE
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

    load_data_to_table(args.namespace, args.branch)
