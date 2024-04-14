from pyspark.sql import SparkSession


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
