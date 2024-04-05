import argparse
import tarfile
from io import BytesIO

from dotenv import load_dotenv
from pyspark.sql import types as T
from pyspark.sql import functions as F

from shared.adls_wrapper import AdlsWrapper
from shared.common import setup_spark_environment
from shared.key_vault_wrapper import KeyVaultWrapper


def run(namespace: str, branch: str):
    source_folder = "landing"
    destination_folder = "processed"

    extract_files(source_folder, destination_folder)
    load_data_to_table(namespace, branch, source_folder)


def extract_files(source_folder: str, destination_folder: str):
    kv_wrapper = KeyVaultWrapper()
    adls_wrapper = AdlsWrapper(kv_wrapper)

    tar_files: list[str] = adls_wrapper.list_tar_files(source_folder)
    for tar_file in tar_files:
        file_bytes = BytesIO(adls_wrapper.get_file_content(tar_file))

        with tarfile.open(fileobj=file_bytes) as tar:
            for member in tar.getmembers():
                content: tarfile.TarInfo = tar.extractfile(member)
                adls_wrapper.upload_bytes(f"{source_folder}/{member.name}", content)

        file_parts = tar_file.split("/")
        file_parts.insert(-1, destination_folder)
        destination = "/".join(file_parts)

        adls_wrapper.move_file(tar_file, destination)


def load_data_to_table(namespace: str, branch: str, location: str):
    spark = setup_spark_environment(namespace, branch)

    df = spark.read.json(
        f"/{location}/ADVANCED/*",
        recursiveFileLookup=True,
        schema=load_schema(),
    )
    df = df.select(
        F.col("pt"),
        F.to_timestamp(F.col("pt") / 1000).alias("timestamp"),
        F.explode(F.col("mc")).alias("mc"),
    )

    df.write.format("iceberg").mode("overwrite").save("raw")


def load_schema() -> T.StructType:
    schema = T.StructType(
        [
            T.StructField("clk", T.StringType(), True),
            T.StructField(
                "mc",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("id", T.StringType(), True),
                            T.StructField(
                                "marketDefinition",
                                T.StructType(
                                    [
                                        T.StructField("betDelay", T.LongType(), True),
                                        T.StructField(
                                            "bettingType", T.StringType(), True
                                        ),
                                        T.StructField(
                                            "bspMarket", T.BooleanType(), True
                                        ),
                                        T.StructField(
                                            "bspReconciled", T.BooleanType(), True
                                        ),
                                        T.StructField(
                                            "complete", T.BooleanType(), True
                                        ),
                                        T.StructField(
                                            "countryCode", T.StringType(), True
                                        ),
                                        T.StructField(
                                            "crossMatching", T.BooleanType(), True
                                        ),
                                        T.StructField(
                                            "discountAllowed", T.BooleanType(), True
                                        ),
                                        T.StructField("eventId", T.StringType(), True),
                                        T.StructField(
                                            "eventName", T.StringType(), True
                                        ),
                                        T.StructField(
                                            "eventTypeId", T.StringType(), True
                                        ),
                                        T.StructField("inPlay", T.BooleanType(), True),
                                        T.StructField(
                                            "marketBaseRate", T.DoubleType(), True
                                        ),
                                        T.StructField(
                                            "marketTime", T.StringType(), True
                                        ),
                                        T.StructField(
                                            "marketType", T.StringType(), True
                                        ),
                                        T.StructField("name", T.StringType(), True),
                                        T.StructField(
                                            "numberOfActiveRunners",
                                            T.LongType(),
                                            True,
                                        ),
                                        T.StructField(
                                            "numberOfWinners", T.LongType(), True
                                        ),
                                        T.StructField("openDate", T.StringType(), True),
                                        T.StructField(
                                            "persistenceEnabled",
                                            T.BooleanType(),
                                            True,
                                        ),
                                        T.StructField(
                                            "regulators",
                                            T.ArrayType(T.StringType(), True),
                                            True,
                                        ),
                                        T.StructField(
                                            "runners",
                                            T.ArrayType(
                                                T.StructType(
                                                    [
                                                        T.StructField(
                                                            "hc",
                                                            T.DoubleType(),
                                                            True,
                                                        ),
                                                        T.StructField(
                                                            "id", T.LongType(), True
                                                        ),
                                                        T.StructField(
                                                            "name",
                                                            T.StringType(),
                                                            True,
                                                        ),
                                                        T.StructField(
                                                            "removalDate",
                                                            T.StringType(),
                                                            True,
                                                        ),
                                                        T.StructField(
                                                            "sortPriority",
                                                            T.LongType(),
                                                            True,
                                                        ),
                                                        T.StructField(
                                                            "status",
                                                            T.StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                        T.StructField(
                                            "runnersVoidable", T.BooleanType(), True
                                        ),
                                        T.StructField(
                                            "settledTime", T.StringType(), True
                                        ),
                                        T.StructField("status", T.StringType(), True),
                                        T.StructField(
                                            "suspendTime", T.StringType(), True
                                        ),
                                        T.StructField("timezone", T.StringType(), True),
                                        T.StructField(
                                            "turnInPlayEnabled",
                                            T.BooleanType(),
                                            True,
                                        ),
                                        T.StructField("version", T.LongType(), True),
                                    ]
                                ),
                                True,
                            ),
                            T.StructField(
                                "rc",
                                T.ArrayType(
                                    T.StructType(
                                        [
                                            T.StructField(
                                                "batb",
                                                T.ArrayType(
                                                    T.ArrayType(T.DoubleType(), True),
                                                    True,
                                                ),
                                                True,
                                            ),
                                            T.StructField(
                                                "batl",
                                                T.ArrayType(
                                                    T.ArrayType(T.DoubleType(), True),
                                                    True,
                                                ),
                                                True,
                                            ),
                                            T.StructField("hc", T.DoubleType(), True),
                                            T.StructField("id", T.LongType(), True),
                                            T.StructField("ltp", T.DoubleType(), True),
                                            T.StructField(
                                                "trd",
                                                T.ArrayType(
                                                    T.ArrayType(T.DoubleType(), True),
                                                    True,
                                                ),
                                                True,
                                            ),
                                            T.StructField("tv", T.DoubleType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                            T.StructField("tv", T.DoubleType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            T.StructField("op", T.StringType(), True),
            T.StructField("pt", T.LongType(), True),
        ]
    )

    return schema


if __name__ == "__main__":
    load_dotenv()

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

    run(args.namespace, args.branch)
