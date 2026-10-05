from src.application.ports import DataReader, DataWriter
from src.domain.financial import (
    AccountingNormalizer,
    DataQualityValidator,
    FinancialAggregator,
)


class ETLUseCase:
    def __init__(self, reader: DataReader, writer: DataWriter):
        self.reader = reader
        self.writer = writer

    def execute(self, source_path: str, target_table: str) -> None:
        df_raw = self.reader.read(source_path)
        df_normalized = AccountingNormalizer.normalize(df_raw)
        df_consolidated = FinancialAggregator.aggregate(df_normalized)
        DataQualityValidator.validate(df_consolidated)
        self.writer.write(df_consolidated, target_table)
