from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, LongType, StructField, StructType
from src.application.ports import DataReader, DataWriter
from src.application.use_cases import ETLUseCase
from src.domain.financial import (
    AccountingNormalizer,
    DataQualityValidator,
    FinancialAggregator,
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


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
    df_consolidated = FinancialAggregator.aggregate(df_normalized)

    # Assert validation passes for standard flow
    DataQualityValidator.validate(df_consolidated)

    df_agg = df_consolidated.collect()
    assert len(df_agg) == 1
    row = df_agg[0]
    assert row["vol_credito_concedido"] == 1000.0
    assert row["vol_credito_liquidado"] == 100.0
    assert row["vol_estornado_total"] == 100.0
    assert row["saldo_liquido_real"] == 1000.0 + 100.0 - 100.0


def test_data_quality_success(spark):
    from datetime import date

    schema = StructType(
        [
            StructField("data_contabil", DateType(), True),
            StructField("vol_credito_concedido", DoubleType(), True),
            StructField("vol_credito_liquidado", DoubleType(), True),
            StructField("vol_estornado_total", DoubleType(), True),
            StructField("saldo_liquido_real", DoubleType(), True),
            StructField("qtd_eventos_reversos", LongType(), True),
            StructField("qtd_total_transacoes", LongType(), True),
        ]
    )
    # Standard valid row
    valid_data = [(date(2026, 10, 1), 1000.0, 200.0, 50.0, 1150.0, 1, 5)]
    df_valid = spark.createDataFrame(valid_data, schema=schema)
    # Should not raise any exception
    DataQualityValidator.validate(df_valid)


def test_data_quality_null_date_failure(spark):
    schema = StructType(
        [
            StructField("data_contabil", DateType(), True),
            StructField("vol_credito_concedido", DoubleType(), True),
            StructField("vol_credito_liquidado", DoubleType(), True),
            StructField("vol_estornado_total", DoubleType(), True),
            StructField("saldo_liquido_real", DoubleType(), True),
            StructField("qtd_eventos_reversos", LongType(), True),
            StructField("qtd_total_transacoes", LongType(), True),
        ]
    )
    invalid_data = [(None, 1000.0, 200.0, 50.0, 1150.0, 1, 5)]
    df_invalid = spark.createDataFrame(invalid_data, schema=schema)

    with pytest.raises(ValueError, match="null 'data_contabil'"):
        DataQualityValidator.validate(df_invalid)


def test_data_quality_math_mismatch_failure(spark):
    from datetime import date

    schema = StructType(
        [
            StructField("data_contabil", DateType(), True),
            StructField("vol_credito_concedido", DoubleType(), True),
            StructField("vol_credito_liquidado", DoubleType(), True),
            StructField("vol_estornado_total", DoubleType(), True),
            StructField("saldo_liquido_real", DoubleType(), True),
            StructField("qtd_eventos_reversos", LongType(), True),
            StructField("qtd_total_transacoes", LongType(), True),
        ]
    )
    # Expected saldo: 1000 + 200 - 50 = 1150. Provided: 9999.0
    invalid_data = [(date(2026, 10, 1), 1000.0, 200.0, 50.0, 9999.0, 1, 5)]
    df_invalid = spark.createDataFrame(invalid_data, schema=schema)

    with pytest.raises(ValueError, match="mathematical inconsistency"):
        DataQualityValidator.validate(df_invalid)


def test_etl_use_case_aborts_on_quality_failure(spark):
    data = [
        {
            "event_type": "CONTRATO_EMITIDO",
            "timestamp_utc": "invalid_date_format",  # to_date(...) returns null
            "partition_key": "C1",
            "data": {"valor_contratado": 1000.0},
        }
    ]
    df_raw = spark.createDataFrame(data)

    mock_reader = MagicMock(spec=DataReader)
    mock_reader.read.return_value = df_raw

    mock_writer = MagicMock(spec=DataWriter)

    use_case = ETLUseCase(mock_reader, mock_writer)

    with pytest.raises(ValueError, match="null 'data_contabil'"):
        use_case.execute("dummy_path", "dummy_table")

    # Verify writer was NOT called when Data Quality Gate fails
    mock_writer.write.assert_not_called()
