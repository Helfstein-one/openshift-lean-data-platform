import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Callable, Tuple
from .event import FinancialEvent

SEGMENTOS = ["VAREJO", "EMPRESAS", "CORPORATE", "AGRO"]
CANAL_ORIGEM = ["APP_MOBILE", "INTERNET_BANKING", "CORRESPONDENTE", "AGENCIA"]
MOTIVOS_CANCELAMENTO = ["DESISTENCIA_CLIENTE", "SCORE_INSUFICIENTE", "EXPIRACAO_PRAZO"]
MOTIVOS_ESTORNO = ["PAGAMENTO_DUPLICADO", "VALOR_INCORRETO", "FRAUDE_CONFIRMADA"]
CODIGOS_MED = ["MD01_SUSPEITA_FRAUDE", "MD02_FALHA_OPERACIONAL", "MD03_COACAO_SEQUESTRO"]
MOTIVOS_CONTESTACAO = ["TRANSACAO_NAO_RECONHECIDA", "GOLPE_ENGENHARIA_SOCIAL"]

PRODUTOS_CREDITO = [
    {"codigo": "CDC_AUTO", "taxa_min": 0.015, "taxa_max": 0.025, "prazo": 60},
    {"codigo": "CRED_PESSOAL", "taxa_min": 0.025, "taxa_max": 0.055, "prazo": 48},
    {"codigo": "CONSIGNADO", "taxa_min": 0.012, "taxa_max": 0.019, "prazo": 84},
    {"codigo": "GIRO_EMPRESAS", "taxa_min": 0.018, "taxa_max": 0.035, "prazo": 36}
]

class EventGenerator:
    @staticmethod
    def generate_proposta(client_id: str) -> Dict[str, Any]:
        p = random.choice(PRODUTOS_CREDITO)
        return {"event_type": "PROPOSTA_CREDITO", "payload": {
            "proposta_id": str(uuid.uuid4()), "cliente_id": client_id, "segmento": random.choice(SEGMENTOS),
            "produto": p["codigo"], "valor_solicitado": round(random.uniform(2000.0, 150000.0), 2),
            "prazo_meses": p["prazo"], "score_serasa": random.randint(320, 990), "status": "EM_ANALISE",
            "canal_origem": random.choice(CANAL_ORIGEM)
        }}

    @staticmethod
    def generate_cancelamento(client_id: str) -> Dict[str, Any]:
        return {"event_type": "CANCELAMENTO_PROPOSTA", "payload": {
            "cancelamento_id": str(uuid.uuid4()), "proposta_id": str(uuid.uuid4()), "cliente_id": client_id,
            "motivo": random.choice(MOTIVOS_CANCELAMENTO), "responsavel": "SISTEMA_RISCO"
        }}

    @staticmethod
    def generate_contrato(client_id: str) -> Dict[str, Any]:
        p = random.choice(PRODUTOS_CREDITO)
        v = round(random.uniform(5000.0, 100000.0), 2)
        tx = round(random.uniform(p["taxa_min"], p["taxa_max"]), 4)
        return {"event_type": "CONTRATO_EMITIDO", "payload": {
            "contrato_num": random.randint(100000000, 999999999), "cliente_id": client_id, "produto": p["codigo"],
            "valor_contratado": v, "taxa_mensal": tx, "prazo_total_meses": p["prazo"],
            "valor_parcela_estimada": round(v * (tx / (1 - (1 + tx)**-p["prazo"])), 2),
            "data_desembolso": datetime.now(timezone.utc).strftime("%Y-%m-%d")
        }}

    @staticmethod
    def generate_pagamento(client_id: str) -> Dict[str, Any]:
        v = round(random.uniform(250.0, 3500.0), 2)
        j = round(v * random.uniform(0.18, 0.40), 2)
        return {"event_type": "PAGAMENTO_PARCELA", "payload": {
            "liquidacao_id": str(uuid.uuid4()), "contrato_num": random.randint(100000000, 999999999),
            "cliente_id": client_id, "parcela_numero": random.randint(1, 48), "valor_pago": v,
            "amortizacao_capital": round(v - j, 2), "juros_apropriados": j,
            "forma_liquidacao": random.choice(["BOLETO", "PIX", "DEBITO_CONTA"])
        }}

    @staticmethod
    def generate_estorno_parc(client_id: str) -> Dict[str, Any]:
        v = round(random.uniform(250.0, 3500.0), 2)
        return {"event_type": "ESTORNO_PARCELA", "payload": {
            "estorno_id": str(uuid.uuid4()), "liquidacao_origem_id": str(uuid.uuid4()),
            "contrato_num": random.randint(100000000, 999999999), "cliente_id": client_id,
            "valor_estornado": v, "motivo_estorno": random.choice(MOTIVOS_ESTORNO),
            "reversao_amortizacao": round(v * 0.75, 2), "reversao_juros": round(v * 0.25, 2)
        }}

    @staticmethod
    def generate_pix(client_id: str) -> Dict[str, Any]:
        return {"event_type": "TRANSACAO_PIX", "payload": {
            "end_to_end_id": f"E{random.randint(10000000,99999999)}{uuid.uuid4().hex[:14]}",
            "pagador_id": client_id, "recebedor_id": f"CLI-{random.randint(1000, 2500)}",
            "valor": round(random.uniform(10.0, 4500.0), 2),
            "tipo_chave": random.choice(["CPF", "CNPJ", "EMAIL", "TELEFONE", "ALEATORIA"])
        }}

    @staticmethod
    def generate_estorno_pix(client_id: str) -> Dict[str, Any]:
        return {"event_type": "ESTORNO_PIX", "payload": {
            "devolucao_id": str(uuid.uuid4()), "end_to_end_original": f"E{random.randint(10000000,99999999)}{uuid.uuid4().hex[:14]}",
            "solicitante_id": client_id, "valor_devolvido": round(random.uniform(10.0, 2500.0), 2),
            "codigo_motivo_med": random.choice(CODIGOS_MED)
        }}

    @staticmethod
    def generate_contestacao(client_id: str) -> Dict[str, Any]:
        return {"event_type": "CONTESTACAO_TRANSACAO", "payload": {
            "disputa_id": str(uuid.uuid4()), "transacao_referencia_id": str(uuid.uuid4()),
            "cliente_id": client_id, "valor_contestado": round(random.uniform(50.0, 8000.0), 2),
            "motivo": random.choice(MOTIVOS_CONTESTACAO), "bloqueio_cautelar_ativo": True
        }}

    @staticmethod
    def generate_renegociacao(client_id: str) -> Dict[str, Any]:
        s = round(random.uniform(5000.0, 50000.0), 2)
        d = round(s * random.uniform(0.10, 0.40), 2)
        return {"event_type": "RENEGOCIACAO_DIVIDA", "payload": {
            "renegociacao_id": str(uuid.uuid4()), "cliente_id": client_id,
            "contratos_consolidados": [random.randint(100000000, 999999999)],
            "saldo_devedor_original": s, "desconto_concedido": d, "novo_saldo_devedor": round(s - d, 2)
        }}

    @staticmethod
    def generate_inadimplencia(client_id: str) -> Dict[str, Any]:
        return {"event_type": "INADIMPLENCIA_FLAG", "payload": {
            "alerta_id": str(uuid.uuid4()), "cliente_id": client_id, "contrato_num": random.randint(100000000, 999999999),
            "faixa_atraso": random.choice(["F1_15_A_30", "F2_31_A_60", "F3_61_A_90", "F4_MAIOR_90"]),
            "saldo_vencido": round(random.uniform(500.0, 15000.0), 2)
        }}

    @classmethod
    def create_random_event(cls) -> FinancialEvent:
        handlers = [
            (cls.generate_proposta, 0.15), (cls.generate_cancelamento, 0.05), (cls.generate_contrato, 0.12),
            (cls.generate_pagamento, 0.25), (cls.generate_estorno_parc, 0.04), (cls.generate_pix, 0.22),
            (cls.generate_estorno_pix, 0.05), (cls.generate_contestacao, 0.04), (cls.generate_renegociacao, 0.04),
            (cls.generate_inadimplencia, 0.04)
        ]
        funcs, weights = zip(*handlers)
        client_id = f"CLI-{random.randint(1000, 2500)}"
        event_dict = random.choices(funcs, weights=weights)[0](client_id)
        
        return FinancialEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_dict["event_type"],
            partition_key=client_id,
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            payload=event_dict["payload"]
        )
