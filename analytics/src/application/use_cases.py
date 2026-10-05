from typing import Optional

from src.application.ports import DataReader, DataWriter, DLQWriter
from src.domain.financial import (
    AccountingNormalizer,
    DLQSegregator,
    FinancialAggregator,
)


class ETLUseCase:
    def __init__(
        self,
        reader: DataReader,
        writer: DataWriter,
        dlq_writer: Optional[DLQWriter] = None,
    ):
        self.reader = reader
        self.writer = writer
        self.dlq_writer = dlq_writer

    def execute(self, source_path: str, target_table: str) -> None:
        df_raw = self.reader.read(source_path)
        df_valid, df_invalid = DLQSegregator.filter_valid_and_invalid(df_raw)

        if self.dlq_writer and not df_invalid.rdd.isEmpty():
            self.dlq_writer.write_dlq(df_invalid)

        df_normalized = AccountingNormalizer.normalize(df_valid)
        df_consolidated = FinancialAggregator.aggregate(df_normalized)
        self.writer.write(df_consolidated, target_table)
