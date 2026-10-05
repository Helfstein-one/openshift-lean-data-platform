#!/usr/bin/env bash
set -euo pipefail

helm repo add superset https://apache.github.io/superset
helm repo update

# Otimização de footprint de memória do Apache Superset no OpenShift Local (CRC):
# - Redução de Gunicorn workers de ~10 (padrão) para 2 trabalhadores com 4 threads
# - Limite rígido de memória em 512Mi (garantindo consumo abaixo de 600MiB de RAM)
# - Desativação de workers em background (supersetWorker) e agendador Celery Beat (supersetCeleryBeat)

helm upgrade --install superset superset/superset \
  --namespace data-platform \
  --set service.type=ClusterIP \
  --set postgresql.resources.requests.cpu=100m \
  --set postgresql.resources.requests.memory=64Mi \
  --set postgresql.resources.limits.cpu=500m \
  --set postgresql.resources.limits.memory=256Mi \
  --set resources.requests.cpu=100m \
  --set resources.requests.memory=256Mi \
  --set resources.limits.cpu=500m \
  --set resources.limits.memory=512Mi \
  --set supersetNode.gunicorn.workers=2 \
  --set supersetNode.gunicorn.threads=4 \
  --set extraEnv.GUNICORN_WORKERS="2" \
  --set extraEnv.SERVER_WORKER_AMOUNT="2" \
  --set init.createAdmin=true \
  --set init.adminUser.username=admin \
  --set init.adminUser.password=admin123 \
  --set supersetWorker.enabled=false \
  --set supersetCeleryBeat.enabled=false

oc create route edge superset --service=superset --port=8088 -n data-platform || true
