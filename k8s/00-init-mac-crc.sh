#!/usr/bin/env bash
set -euo pipefail

echo "==> [1/4] Ajustando configurações do CRC para o MacBook Air M4..."
crc config set cpus 6
crc config set memory 11776
crc config set consent-telemetry no

echo "==> [2/4] Inicializando o cluster..."
crc setup
crc start

echo "==> [3/4] Autenticando com oc CLI..."
eval $(crc oc-env)
OC_PASS=$(crc console --credentials | grep kubeadmin | awk -F'-p ' '{print $2}' | awk '{print $1}')
oc login -u kubeadmin -p "${OC_PASS}" https://api.crc.testing:6443

echo "==> [4/4] Suprimindo Cluster Monitoring para liberar 2 GB de RAM..."
oc patch clusterversion version --type json -p '[
  {"op": "add", "path": "/spec/overrides", "value": [
    {"group": "apps", "kind": "Deployment", "name": "prometheus-operator", "namespace": "openshift-monitoring", "unmanaged": true}
  ]}
]'
oc scale deployment -n openshift-monitoring prometheus-operator --replicas=0 || true
oc scale statefulset -n openshift-monitoring prometheus-k8s --replicas=0 || true

oc new-project data-platform || oc project data-platform
echo "==> Cluster pronto para carga de dados."
