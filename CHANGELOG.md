# Changelog

## [Unreleased]
### Added
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
