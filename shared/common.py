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


@staticmethod
def get_flattened_df(spark: SparkSession):
    if not spark.catalog.tableExists("flattened_view"):
        create_flattened_df(spark)

    return spark.read.table("flattened_view")


def create_flattened_df(spark: SparkSession):
    df: DataFrame = spark.read.table("raw")

    flattened_df: DataFrame = (
        df.select(F.col("pt"), F.col("timestamp"), F.col("mc.*"))
        .where(F.col("mc.marketDefinition").isNotNull())
        .select(
            F.col("id"), F.col("pt"), F.col("timestamp"), F.col("marketDefinition.*")
        )
    )

    flattened_df.createTempView("flattened_view")
