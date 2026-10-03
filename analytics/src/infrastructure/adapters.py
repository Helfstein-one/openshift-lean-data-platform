from pyspark.sql import DataFrame, SparkSession
from src.application.ports import DataReader, DataWriter


class S3JsonReader(DataReader):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: str) -> DataFrame:
        return self.spark.read.json(path)


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
