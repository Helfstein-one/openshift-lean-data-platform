from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import to_date, when
from pyspark.sql.types import DoubleType


class AccountingNormalizer:
    @staticmethod
    def normalize(df_raw: DataFrame) -> DataFrame:
        return df_raw.select(
            col("event_type"),
            to_date(col("timestamp_utc")).alias("data_contabil"),
            col("partition_key").alias("cliente_id"),
            when(
                col("event_type") == "PAGAMENTO_PARCELA",
                col("data.valor_pago").cast(DoubleType()),
            )
            .when(
                col("event_type") == "TRANSACAO_PIX",
                col("data.valor").cast(DoubleType()),
            )
            .when(
                col("event_type") == "CONTRATO_EMITIDO",
                col("data.valor_contratado").cast(DoubleType()),
            )
            .when(
                col("event_type") == "ESTORNO_PARCELA",
                -col("data.valor_estornado").cast(DoubleType()),
            )
            .when(
                col("event_type") == "ESTORNO_PIX",
                -col("data.valor_devolvido").cast(DoubleType()),
            )
            .when(
                col("event_type") == "CONTESTACAO_TRANSACAO",
                -col("data.valor_contestado").cast(DoubleType()),
            )
            .otherwise(0.0)
            .alias("valor_movimento"),
            when(
                col("event_type").isin(
                    "ESTORNO_PARCELA", "ESTORNO_PIX", "CONTESTACAO_TRANSACAO"
                ),
                1,
            )
            .otherwise(0)
            .alias("flg_reversao"),
        )


class FinancialAggregator:
    @staticmethod
    def aggregate(df_normalized: DataFrame) -> DataFrame:
        return df_normalized.groupBy("data_contabil").agg(
            _sum(
                when(
                    col("event_type") == "CONTRATO_EMITIDO", col("valor_movimento")
                ).otherwise(0.0)
            ).alias("vol_credito_concedido"),
            _sum(
                when(
                    col("event_type").isin("PAGAMENTO_PARCELA", "TRANSACAO_PIX"),
                    col("valor_movimento"),
                ).otherwise(0.0)
            ).alias("vol_credito_liquidado"),
            _sum(
                when(col("flg_reversao") == 1, -col("valor_movimento")).otherwise(0.0)
            ).alias("vol_estornado_total"),
            _sum("valor_movimento").alias("saldo_liquido_real"),
            _sum("flg_reversao").alias("qtd_eventos_reversos"),
            count("*").alias("qtd_total_transacoes"),
        )
