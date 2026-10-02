# OpenShift Lean Data Platform

Uma plataforma de dados bancária e analítica de ponta a ponta, nativa em contêineres e orientada a eventos, projetada para operar no limite de recursos de um ambiente **OpenShift Local (CRC)** em Apple Silicon.

---

## 1. O que é o OpenShift e como ele roda aqui?

O **Red Hat OpenShift** é uma plataforma de aplicação em contêineres Kubernetes corporativa. O **OpenShift Local (anteriormente CodeReady Containers - CRC)** traz um cluster OpenShift minimalista (Node único) que pode rodar em uma máquina de desenvolvedor. 

Neste projeto, o OpenShift é executado através de uma Máquina Virtual (VM) fornecida pelo CRC no macOS. Essa VM aloca uma quantidade fixa de vCPUs e Memória RAM. O OpenShift por si só (Control Plane, Operators, DNS, Ingress) consome uma quantidade massiva de recursos.

Para viabilizar uma plataforma de Big Data inteira (Kafka, MinIO, Spark, PostgreSQL, Superset) nessa mesma VM restrita, foi necessário um design de **"Orçamento Cirúrgico de Memória"**, reduzindo os "Requests" e "Limits" de CPU e RAM no limite da margem de erro, e até mesmo suspendendo serviços nativos do OpenShift (como Monitoramento, Registry e CVO).

## 2. Visão da Plataforma e Kubernetes (K8s)

O **Kubernetes (K8s)** é o orquestrador subjacente do OpenShift. Todos os componentes desta arquitetura rodam como **Pods** controlados por recursos nativos do K8s:

- **Storage S3 (Landing Zone):** Um servidor **MinIO** rodando em um único Pod (via `Deployment`), consumindo o mínimo de memória.
- **Mensageria Event-Driven:** **Apache Kafka** em modo KRaft (sem Zookeeper) implantado e gerenciado através do operador **Strimzi**.
- **Data Mart Analítico:** Um banco **PostgreSQL** provisionado pelo Helm Chart do **Apache Superset**, também modificado para não usar background workers pesados.
- **Micro-batch Processing:** Em vez de usar clusters Spark dedicados ou o Apache Airflow, a computação de consolidação usa **CronJobs** nativos do Kubernetes. O K8s cria contêineres efêmeros a cada duas horas que sobem o PySpark, processam dados do MinIO, gravam no Postgres e morrem.

## 3. Arquitetura de Software do Projeto (SOLID & Hexagonal)

Para garantir qualidade, o código-fonte que era apenas "scripts" foi refatorado utilizando **Arquitetura Hexagonal (Ports and Adapters)** e os princípios **SOLID**. O repositório foi dividido em serviços menores:

### A. Módulo `producer/`
Um microsserviço Python que simula um Core Bancário gerando eventos financeiros (Propostas de Crédito, Contratos, Pagamentos, PIX, Estornos).
- **Domain:** Entidades (`FinancialEvent`) e lógicas geradoras isoladas (`generators.py`). Não conhecem Kafka ou infraestrutura.
- **Application:** Os "Ports" (`MessagePublisher`) e "Use Cases" (`ProduceEventsUseCase`), garantindo o princípio de Inversão de Dependência (DIP).
- **Infrastructure:** Os adaptadores que conectam a aplicação ao mundo externo (`KafkaMessagePublisher`), injetados no Use Case pelo arquivo principal (`main.py`).

### B. Módulo `analytics/`
Um trabalho de dados (ETL) escrito em PySpark.
- **Domain:** O núcleo financeiro (`AccountingNormalizer`, `FinancialAggregator`) onde os Estornos, Cancelamentos e Disputas (MED) entram abatendo (negativamente) os saldos contábeis gerando o fluxo de "Reversibilidade Contábil".
- **Application:** Portas que definem abstrações de Leitura/Escrita (`DataReader`, `DataWriter`) e o `ETLUseCase`.
- **Infrastructure:** Leitores S3 (`S3JsonReader`) e Escritores JDBC (`PostgresJDBCWriter`).

## 4. Estrutura de CI/CD Granular

O repositório possui uma árvore de automação no GitHub Actions que entende os limites de contexto:
- `.github/workflows/ci-producer.yml`: Roda os testes unitários da pasta `producer` **apenas** quando há *commit* dentro daquela pasta.
- `.github/workflows/ci-analytics.yml`: Roda os testes do PySpark **apenas** quando há alterações na pasta `analytics`.

## 5. Como Executar o Projeto

1. **Inicie o OpenShift CRC Local:**
   ```bash
   ./k8s/00-init-mac-crc.sh
   eval $(crc oc-env)
   ```

2. **Libere os Privilégios de Segurança (SCC AnyUID):**
   *(PostgreSQL e Apache Spark exigem permissão para rodar com usuários root ou UIDs estáticos)*
   ```bash
   oc adm policy add-scc-to-user anyuid -z default -n data-platform
   oc adm policy add-scc-to-user anyuid -z superset -n data-platform
   oc adm policy add-scc-to-user anyuid -z superset-postgresql -n data-platform
   oc adm policy add-scc-to-user anyuid -z spark-sa -n data-platform
   ```

3. **Suba a Camada de Armazenamento e Mensageria:**
   ```bash
   oc apply -f k8s/01-minio-lean.yaml -f k8s/02-kafka-kraft-lean.yaml
   ```

4. **Implante a Camada de Apresentação e Banco de Dados (Helm):**
   ```bash
   ./k8s/03-superset-postgresql.sh
   ```

5. **Implante as Aplicações de Negócio:**
   ```bash
   oc apply -f k8s/04-producer-deployment.yaml -f k8s/05-spark-analytics.yaml
   ```

6. **Acionando o ETL Analítico Manualmente:**
   O processamento é agendado para cada 2h, mas pode ser executado manualmente:
   ```bash
   oc create job --from=cronjob/spark-analytics-cron spark-manual-run -n data-platform
   ```
