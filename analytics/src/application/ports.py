from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class DataReader(ABC):
    @abstractmethod
    def read(self, path: str) -> DataFrame:
        pass


class DataWriter(ABC):
    @abstractmethod
    def write(self, df: DataFrame, table: str) -> None:
        pass


class DLQWriter(ABC):
    @abstractmethod
    def write_dlq(self, df_invalid: DataFrame) -> None:
        pass
