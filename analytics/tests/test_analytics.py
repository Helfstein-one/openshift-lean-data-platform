import pytest
from pyspark.sql import SparkSession
from src.application.use_cases import ETLUseCase
from src.domain.financial import (
    AccountingNormalizer,
    DLQSegregator,
    FinancialAggregator,
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


class MockDataReader:
    def __init__(self, df):
        self.df = df

    def read(self, path: str):
        return self.df


class MockDataWriter:
    def __init__(self):
        self.written_df = None
        self.table = None

    def write(self, df, table: str):
        self.written_df = df
        self.table = table


class MockDLQWriter:
    def __init__(self):
        self.dlq_df = None

    def write_dlq(self, df_invalid):
        self.dlq_df = df_invalid


def test_normalize_and_aggregate(spark):
    data = [
        {
            "event_type": "CONTRATO_EMITIDO",
            "timestamp_utc": "2026-10-01T10:00:00Z",
            "partition_key": "C1",
            "data": {"valor_contratado": 1000.0},
        },
        {
            "event_type": "PAGAMENTO_PARCELA",
            "timestamp_utc": "2026-10-01T11:00:00Z",
            "partition_key": "C1",
            "data": {"valor_pago": 100.0},
        },
        {
            "event_type": "ESTORNO_PARCELA",
            "timestamp_utc": "2026-10-01T12:00:00Z",
            "partition_key": "C1",
            "data": {"valor_estornado": 100.0},
        },
    ]
    df_raw = spark.createDataFrame(data)

    df_normalized = AccountingNormalizer.normalize(df_raw)
    df_agg = FinancialAggregator.aggregate(df_normalized).collect()

    assert len(df_agg) == 1
    row = df_agg[0]
    assert row["vol_credito_concedido"] == 1000.0
    assert row["vol_credito_liquidado"] == 100.0
    assert row["vol_estornado_total"] == 100.0
    assert row["saldo_liquido_real"] == 1000.0 + 100.0 - 100.0


def test_dlq_segregator(spark):
    data = [
        {
            "event_type": "CONTRATO_EMITIDO",
            "timestamp_utc": "2026-10-01T10:00:00Z",
            "partition_key": "C1",
            "_corrupt_record": None,
        },
        {
            "event_type": None,
            "timestamp_utc": "2026-10-01T11:00:00Z",
            "partition_key": "C1",
            "_corrupt_record": None,
        },
        {
            "event_type": "PAGAMENTO_PARCELA",
            "timestamp_utc": None,
            "partition_key": "C1",
            "_corrupt_record": "{bad json}",
        },
    ]
    df_raw = spark.createDataFrame(data)

    df_valid, df_invalid = DLQSegregator.filter_valid_and_invalid(df_raw)

    assert df_valid.count() == 1
    assert df_invalid.count() == 2


def test_dlq_segregator_corrupt_only(spark):
    data = [{"_corrupt_record": "{corrupted_json_without_other_cols}"}]
    df_raw = spark.createDataFrame(data)

    df_valid, df_invalid = DLQSegregator.filter_valid_and_invalid(df_raw)

    assert df_valid.count() == 0
    assert df_invalid.count() == 1


def test_etl_use_case_with_dlq(spark):
    data = [
        {
            "event_type": "CONTRATO_EMITIDO",
            "timestamp_utc": "2026-10-01T10:00:00Z",
            "partition_key": "C1",
            "data": {"valor_contratado": 1000.0},
            "_corrupt_record": None,
        },
        {
            "event_type": None,
            "timestamp_utc": "2026-10-01T11:00:00Z",
            "partition_key": "C1",
            "data": None,
            "_corrupt_record": "invalid json line",
        },
    ]
    df_raw = spark.createDataFrame(data)

    reader = MockDataReader(df_raw)
    writer = MockDataWriter()
    dlq_writer = MockDLQWriter()

    use_case = ETLUseCase(reader, writer, dlq_writer)
    use_case.execute("s3a://dummy", "target_table")

    assert writer.written_df is not None
    assert dlq_writer.dlq_df is not None
    assert dlq_writer.dlq_df.count() == 1
