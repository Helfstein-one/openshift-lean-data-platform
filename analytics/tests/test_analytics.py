import pytest
from pyspark.sql import SparkSession
from src.domain.financial import AccountingNormalizer, FinancialAggregator


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
    df_agg = FinancialAggregator.aggregate(df_normalized).collect()

    assert len(df_agg) == 1
    row = df_agg[0]
    assert row["vol_credito_concedido"] == 1000.0
    assert row["vol_credito_liquidado"] == 100.0
    assert row["vol_estornado_total"] == 100.0
    assert row["saldo_liquido_real"] == 1000.0 + 100.0 - 100.0
