CREATE TABLE IF NOT EXISTS dlq_error_logs (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100),
    error_reason TEXT,
    raw_payload TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW vw_kpis_risco_chargeback AS
SELECT 
    data_contabil,
    vol_credito_concedido,
    vol_credito_liquidado,
    vol_estornado_total,
    saldo_liquido_real,
    qtd_eventos_reversos,
    qtd_total_transacoes,
    -- Taxa de Reversão / Chargeback Operacional (%)
    ROUND(
        (vol_estornado_total / NULLIF(vol_credito_liquidado + vol_estornado_total, 0) * 100)::numeric, 
        2
    ) AS taxa_chargeback_med_pct,
    -- Índice de Severidade de Disputa por Transação
    ROUND(
        (vol_estornado_total / NULLIF(qtd_eventos_reversos, 0))::numeric, 
        2
    ) AS ticket_medio_estorno
FROM faturamento_contabil_diario;
