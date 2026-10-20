# Changelog

## [Unreleased]
### Added
- **Arquitetura Hexagonal & SOLID no Módulo de Analytics:**
  - Introdução da porta [`SilverTableWriter`](file:///Users/mauriciohelfstein/dev/openshift-lean-data-platform/analytics/src/application/ports.py) e adapter [`IcebergSilverWriter`](file:///Users/mauriciohelfstein/dev/openshift-lean-data-platform/analytics/src/infrastructure/adapters.py) desacoplando o motor transacional de armazenamento.
  - Orquestração no [`ETLUseCase`](file:///Users/mauriciohelfstein/dev/openshift-lean-data-platform/analytics/src/application/use_cases.py) aderente ao Princípio da Inversão de Dependência (DIP) e Segregação de Interfaces (ISP).
  - Camada de domínio contábil pura isolada no [`financial.py`](file:///Users/mauriciohelfstein/dev/openshift-lean-data-platform/analytics/src/domain/financial.py) sem acoplamento a drivers externos (SRP).
- **Camada Lakehouse Apache Iceberg (Medallion Bronze ➔ Silver ➔ Gold):**
  - Armazenamento transacional ACID na camada Silver (`lakehouse.silver_transacoes_financeiras`) em Parquet com suporte a Time Travel e isolamento de commits.
  - Catálogo JDBC integrado diretamente no PostgreSQL existente (Zero RAM adicional na VM do OpenShift CRC).
  - Otimização de escrita em Copy-on-Write (CoW) compatível com o ciclo de vida efêmero do Spark CronJob.
- Dead Letter Queue (DLQ) para segregação de eventos malformados e registros corrompidos no Kafka e PostgreSQL (`dlq_error_logs`).
- Pool de conexões transacional leve com PgBouncer na porta 6432 para o PostgreSQL.
- Otimização de footprint do Apache Superset (<600MiB RAM) via Gunicorn tuning (2 workers, 4 threads) e desativação de Celery background pods.
- Ajuste fino e JVM heap tuning (-Xms256m, -Xmx256m, G1GC, ExitOnOutOfMemoryError) para Kafka KRaft e Kafka Connect S3 Sink Connector.
- Documentação aprofundada no `README.md` detalhando as decisões arquiteturais da stack (Kafka KRaft, MinIO, PySpark CronJob, PgBouncer, Superset).
- Gate de Qualidade de Dados (Data Quality) no script de Reversibilidade Contábil (`analytics/src/domain/financial.py` e k8s) para validação antecipada.
- Otimização do pipeline de CI (`ci-producer.yml` e `ci-analytics.yml`) com cache de dependências Python.

## [1.0.0] - 2026-10-01
### Added
- Separação de módulos em `producer` e `analytics`.
- Refatoração para Arquitetura Hexagonal e SOLID.
- Suíte de testes unitários com pytest e pytest-mock.
- CI/CD granular via GitHub Actions.
- Manifestos Kubernetes isolados na pasta `k8s/`.
- Permissões de SCC configuradas para execução em OpenShift.
