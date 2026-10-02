#!/usr/bin/env bash
set -euo pipefail

helm repo add superset https://apache.github.io/superset
helm repo update

helm upgrade --install superset superset/superset \
  --namespace data-platform \
  --set service.type=ClusterIP \
  --set postgresql.resources.requests.cpu=100m \
  --set postgresql.resources.requests.memory=64Mi \
  --set postgresql.resources.limits.cpu=500m \
  --set postgresql.resources.limits.memory=256Mi \
  --set resources.requests.cpu=200m \
  --set resources.requests.memory=256Mi \
  --set resources.limits.cpu=1000m \
  --set resources.limits.memory=512Mi \
  --set init.createAdmin=true \
  --set init.adminUser.username=admin \
  --set init.adminUser.password=admin123 \
  --set supersetWorker.enabled=false   # Desativa workers em background para poupar 500 MiB de RAM

oc create route edge superset --service=superset --port=8088 -n data-platform || true
