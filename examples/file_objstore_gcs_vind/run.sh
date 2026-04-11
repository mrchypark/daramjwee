#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_NAME="${DARAMJWEE_VCLUSTER_NAME:-daramjwee-local}"
CLUSTER_NAMESPACE="${DARAMJWEE_VCLUSTER_NAMESPACE:-${CLUSTER_NAME}}"
LOCAL_PORT="${DARAMJWEE_GCS_LOCAL_PORT:-4443}"
SERVICE_NAME="gcsemulator"
KUBECONFIG_PATH="$(mktemp -t daramjwee-gcs-kubeconfig-XXXXXX)"
PORT_FORWARD_LOG="$(mktemp -t daramjwee-gcs-portforward-XXXXXX.log)"
PORT_FORWARD_PID=""

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

wait_for_port_forward() {
  local pid="$1"
  local log_file="$2"
  local port="$3"
  for _ in $(seq 1 60); do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      echo "kubectl port-forward exited before becoming ready" >&2
      cat "${log_file}" >&2 || true
      return 1
    fi
    if grep -q "Forwarding from 127.0.0.1:${port}" "${log_file}" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for kubectl port-forward on 127.0.0.1:${port}" >&2
  cat "${log_file}" >&2 || true
  return 1
}

cleanup() {
  if [[ -n "${PORT_FORWARD_PID}" ]] && kill -0 "${PORT_FORWARD_PID}" >/dev/null 2>&1; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  fi
  rm -f "${KUBECONFIG_PATH}" "${PORT_FORWARD_LOG}"
}
trap cleanup EXIT

require_cmd docker
require_cmd go
require_cmd kubectl
require_cmd vcluster

if ! vcluster connect "${CLUSTER_NAME}" --namespace "${CLUSTER_NAMESPACE}" --driver docker --print >"${KUBECONFIG_PATH}" 2>/dev/null; then
  vcluster create "${CLUSTER_NAME}" \
    --namespace "${CLUSTER_NAMESPACE}" \
    --driver docker \
    --connect=false \
    --background-proxy=false \
    --create-namespace
  vcluster connect "${CLUSTER_NAME}" --namespace "${CLUSTER_NAMESPACE}" --driver docker --print >"${KUBECONFIG_PATH}"
fi

kubectl --kubeconfig "${KUBECONFIG_PATH}" apply -f "${SCRIPT_DIR}/manifests"
kubectl --kubeconfig "${KUBECONFIG_PATH}" rollout status deployment/"${SERVICE_NAME}" --timeout=180s
kubectl --kubeconfig "${KUBECONFIG_PATH}" port-forward service/"${SERVICE_NAME}" "${LOCAL_PORT}:9000" >"${PORT_FORWARD_LOG}" 2>&1 &
PORT_FORWARD_PID=$!
wait_for_port_forward "${PORT_FORWARD_PID}" "${PORT_FORWARD_LOG}" "${LOCAL_PORT}"

export DARAMJWEE_GCS_EMULATOR_HOST="127.0.0.1:${LOCAL_PORT}"
export DARAMJWEE_GCS_BUCKET="${DARAMJWEE_GCS_BUCKET:-daramjwee-gcs-local}"

cd "${SCRIPT_DIR}"
go run .
