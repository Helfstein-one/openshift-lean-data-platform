from typing import Optional

from src.application.ports import (
    DataReader,
    DataWriter,
    DLQWriter,
    SilverTableWriter,
)
from src.domain.financial import (
    AccountingNormalizer,
    DataQualityValidator,
    DLQSegregator,
    FinancialAggregator,
)


class ETLUseCase:
    def __init__(
        self,
        reader: DataReader,
        writer: DataWriter,
        dlq_writer: Optional[DLQWriter] = None,
        silver_writer: Optional[SilverTableWriter] = None,
    ):
        self.reader = reader
        self.writer = writer
        self.dlq_writer = dlq_writer
        self.silver_writer = silver_writer

    def execute(
        self,
        source_path: str,
        target_table: str,
        silver_table: Optional[str] = None,
    ) -> None:
        df_raw = self.reader.read(source_path)
        df_valid, df_invalid = DLQSegregator.filter_valid_and_invalid(df_raw)

        if self.dlq_writer and not df_invalid.rdd.isEmpty():
            self.dlq_writer.write_dlq(df_invalid)

        df_normalized = AccountingNormalizer.normalize(df_valid)

        if self.silver_writer and silver_table:
            self.silver_writer.write_silver(df_normalized, silver_table)

        df_consolidated = FinancialAggregator.aggregate(df_normalized)
        DataQualityValidator.validate(df_consolidated)
        self.writer.write(df_consolidated, target_table)
