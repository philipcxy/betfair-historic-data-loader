from pyspark.sql import SparkSession


@staticmethod
def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@staticmethod
def set_namespace(spark: SparkSession, namespace: str):
    # Need to create one level at a time
    namespace_parts = namespace.split(".")
    for i in range(1, len(namespace_parts) + 1):
        namespace_level = ".".join(namespace_parts[:i])
        print(f"CREATE NAMESPACE IF NOT EXISTS {namespace_level}")
        print(f"USE {namespace_level}")


@staticmethod
def set_branch(spark: SparkSession, branch: str):
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch} IN betting")
    spark.sql(f"USE REFERENCE {branch}")


@staticmethod
def setup_spark_environment(namespace: str = None, branch: str = None) -> SparkSession:
    spark = get_spark_session()

    if namespace:
        set_namespace(spark, namespace)
    if branch:
        set_branch(spark, namespace)

    return spark
