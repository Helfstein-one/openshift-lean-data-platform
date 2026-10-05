# Changelog

## [Unreleased]
### Added
- Documentação aprofundada no `README.md` detalhando as decisões arquiteturais da stack (Kafka KRaft, MinIO, PySpark CronJob, Superset).
- Gate de Qualidade de Dados (Data Quality) no script de Reversibilidade Contábil (`analytics/src/accounting_engine.py`) para validação antecipada.
- Otimização do pipeline de CI (`ci-producer.yml` e `ci-analytics.yml`) com cache de dependências Python.

## [1.0.0] - 2026-10-01
### Added
- Separação de módulos em `producer` e `analytics`.
- Refatoração para Arquitetura Hexagonal e SOLID.
- Suíte de testes unitários com pytest e pytest-mock.
- CI/CD granular via GitHub Actions.
- Manifestos Kubernetes isolados na pasta `k8s/`.
- Permissões de SCC configuradas para execução em OpenShift.
