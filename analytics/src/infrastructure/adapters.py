import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, struct, to_json, when
from pyspark.sql.types import StringType
from src.application.ports import DataReader, DataWriter, DLQWriter

logger = logging.getLogger(__name__)


class S3JsonReader(DataReader):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: str) -> DataFrame:
        df = (
            self.spark.read.option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(path)
        )
        if "_corrupt_record" not in df.columns:
            df = df.withColumn("_corrupt_record", lit(None).cast(StringType()))
        return df


class PostgresJDBCWriter(DataWriter):
    def __init__(self, url: str, user: str, password: str):
        self.url = url
        self.user = user
        self.password = password

    def write(self, df: DataFrame, table: str) -> None:
        df.write.format("jdbc").option("url", self.url).option("dbtable", table).option(
            "user", self.user
        ).option("password", self.password).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "overwrite"
        ).save()


class PostgresJDBCDLQWriter(DLQWriter):
    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        table: str = "dlq_error_logs",
        topic: str = "events-dlq",
        kafka_bootstrap: Optional[str] = None,
    ):
        self.url = url
        self.user = user
        self.password = password
        self.table = table
        self.topic = topic
        self.kafka_bootstrap = kafka_bootstrap

    def write_dlq(self, df_invalid: DataFrame) -> None:
        if df_invalid.rdd.isEmpty():
            return

        payload_cols = [c for c in df_invalid.columns if c != "_corrupt_record"]
        raw_payload_expr = (
            to_json(struct(*[col(c) for c in payload_cols]))
            if payload_cols
            else lit("{}")
        )

        df_dlq_pg = df_invalid.select(
            lit("spark-analytics").alias("source"),
            when(col("_corrupt_record").isNotNull(), col("_corrupt_record"))
            .otherwise(
                lit(
                    "Missing mandatory envelope fields (event_type, timestamp_utc, partition_key)"
                )
            )
            .alias("error_reason"),
            raw_payload_expr.alias("raw_payload"),
            current_timestamp().alias("created_at"),
        )

        df_dlq_pg.write.format("jdbc").option("url", self.url).option(
            "dbtable", self.table
        ).option("user", self.user).option("password", self.password).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()

        if self.kafka_bootstrap:
            try:
                df_dlq_kafka = df_dlq_pg.select(
                    col("source").alias("key"),
                    to_json(
                        struct("source", "error_reason", "raw_payload", "created_at")
                    ).alias("value"),
                )
                df_dlq_kafka.write.format("kafka").option(
                    "kafka.bootstrap.servers", self.kafka_bootstrap
                ).option("topic", self.topic).save()
            except Exception as e:
                logger.warning(
                    f"Falha ao enviar mensagens invalidas para Kafka DLQ: {e}"
                )
