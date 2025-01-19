from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@staticmethod
def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@staticmethod
def set_namespace(spark: SparkSession, namespace: str):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(f"USE {namespace}")


@staticmethod
def set_branch(spark: SparkSession, branch: str):
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch} IN betting")
    spark.sql(f"USE REFERENCE {branch} IN betting")


@staticmethod
def setup_spark_environment(namespace: str = None, branch: str = None) -> SparkSession:
    spark = get_spark_session()

    if namespace:
        set_namespace(spark, namespace)
    if branch:
        set_branch(spark, branch)

    return spark


def save_table(
    spark: SparkSession, df: DataFrame, table_name: str, mode: str = "append"
):
    if not spark.catalog.tableExists(table_name):
        df.writeTo(table_name).create()
    else:
        match mode:
            case "replace":
                df.writeTo(table_name).replace()
            case "append":
                df.writeTo(table_name).append()


@staticmethod
def get_raw_df(spark: SparkSession):
    """
    TODO: Update function to get latest changes
    e.g.
    spark.read
        .format("iceberg")
        .option("start-snapshot-id", "10963874102873")
        .option("end-snapshot-id", "63874143573109")
        .load("path/to/table")
    """
    if not spark.catalog.tableExists("raw_flattened"):
        create_flattened_df(spark)

    return spark.read.table("raw_flattened")


def create_flattened_df(spark: SparkSession):
    latest_snapshot: int = spark.sql("""
                                     SELECT snapshot_id 
                                     FROM betting.soccer.raw.snapshots 
                                     ORDER BY committed_at 
                                     DESC LIMIT 1
                                     """).first()["snapshot_id"]
    spark.sql(f"""
                CALL betting.system.create_changelog_view(
                    table => 'soccer.raw',
                    options => map('end-snapshot-id', '{latest_snapshot}'),
                                net_changes => true);
                """)

    df: DataFrame = spark.read.table("raw_changes")

    flattened_df: DataFrame = (
        df.select(F.col("pt"), F.col("timestamp"), F.col("mc.*"))
        .where(F.col("mc.marketDefinition").isNotNull())
        .select(
            F.col("id"), F.col("pt"), F.col("timestamp"), F.col("marketDefinition.*")
        )
    )

    flattened_df.write.format("iceberg").mode("overwrite").save("raw_flattened")
