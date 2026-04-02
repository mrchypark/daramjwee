#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKTREE_DIR="$(mktemp -d -t daramjwee-prodlike-v0310-XXXXXX)"
LOG_DIR="$(mktemp -d -t daramjwee-prodlike-logs-XXXXXX)"
AZURITE_DIR="$(mktemp -d -t daramjwee-azurite-XXXXXX)"
AZURITE_CERT_DIR="$(mktemp -d -t daramjwee-azurite-certs-XXXXXX)"
AZURITE_PID=""
AZURITE_HOST="${DJ_AZURITE_HOST:-127.0.0.1}"
AZURITE_PORT="${DJ_AZURITE_PORT:-10000}"
BASELINE_REF="${DJ_COMPARE_BASELINE_REF:-v0.3.10}"

export DJ_RUN_PRODLIKE_COMPARE=1
export DJ_AZURITE_CONNECTION_STRING="${DJ_AZURITE_CONNECTION_STRING:-DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=https://${AZURITE_HOST}:${AZURITE_PORT}/devstoreaccount1;}"
KEEP_ARTIFACTS="${KEEP_ARTIFACTS:-0}"

cleanup() {
  if [[ -n "${AZURITE_PID}" ]] && kill -0 "${AZURITE_PID}" >/dev/null 2>&1; then
    kill "${AZURITE_PID}" >/dev/null 2>&1 || true
    wait "${AZURITE_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -d "${WORKTREE_DIR}" ]]; then
    git -C "${ROOT_DIR}" worktree remove --force "${WORKTREE_DIR}" >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_ARTIFACTS}" != "1" ]]; then
    rm -rf "${LOG_DIR}" "${AZURITE_DIR}" "${AZURITE_CERT_DIR}"
  fi
}
trap cleanup EXIT

listener_pid() {
  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi
  lsof -nP -t -iTCP:"${AZURITE_PORT}" -sTCP:LISTEN 2>/dev/null | head -n 1
}

listener_is_reachable() {
  if command -v lsof >/dev/null 2>&1; then
    [[ -n "$(listener_pid)" ]]
    return
  fi
  (: <>"/dev/tcp/${AZURITE_HOST}/${AZURITE_PORT}") >/dev/null 2>&1
}

listener_supports_tls() {
  openssl s_client \
    -connect "${AZURITE_HOST}:${AZURITE_PORT}" \
    -servername "${AZURITE_HOST}" \
    -brief </dev/null >/dev/null 2>&1
}

listener_looks_like_azurite() {
  printf 'GET /devstoreaccount1?comp=list HTTP/1.1\r\nHost: %s:%s\r\nConnection: close\r\n\r\n' \
    "${AZURITE_HOST}" "${AZURITE_PORT}" |
    openssl s_client \
      -connect "${AZURITE_HOST}:${AZURITE_PORT}" \
      -servername "${AZURITE_HOST}" \
      -quiet 2>/dev/null |
    grep -qi 'azurite'
}

ensure_existing_azurite() {
  if ! listener_supports_tls; then
    echo "port ${AZURITE_PORT} is listening but does not accept TLS; stop it or set DJ_AZURITE_PORT" >&2
    exit 1
  fi
  if ! listener_looks_like_azurite; then
    echo "port ${AZURITE_PORT} is listening and accepts TLS, but it did not identify itself as Azurite; stop it or set DJ_AZURITE_PORT" >&2
    exit 1
  fi
}

if [[ ! -f "${AZURITE_CERT_DIR}/cert.pem" || ! -f "${AZURITE_CERT_DIR}/key.pem" ]]; then
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "${AZURITE_CERT_DIR}/key.pem" \
    -out "${AZURITE_CERT_DIR}/cert.pem" \
    -days 1 \
    -subj "/CN=127.0.0.1" >/dev/null 2>&1
fi

if listener_is_reachable; then
  ensure_existing_azurite
else
  if ! command -v npx >/dev/null 2>&1; then
    echo "npx is required to run Azurite; install Node.js or point DJ_AZURITE_PORT at an existing TLS Azurite listener" >&2
    exit 1
  fi
  npx azurite \
    --blobHost "${AZURITE_HOST}" \
    --blobPort "${AZURITE_PORT}" \
    --location "${AZURITE_DIR}" \
    --cert "${AZURITE_CERT_DIR}/cert.pem" \
    --key "${AZURITE_CERT_DIR}/key.pem" \
    --skipApiVersionCheck \
    >"${LOG_DIR}/azurite.log" 2>&1 &
  AZURITE_PID=$!
  for _ in {1..30}; do
    if ! kill -0 "${AZURITE_PID}" >/dev/null 2>&1; then
      echo "azurite exited before becoming ready; see ${LOG_DIR}/azurite.log" >&2
      exit 1
    fi
    if listener_is_reachable && listener_supports_tls; then
      break
    fi
    sleep 1
  done
  if ! listener_is_reachable || ! listener_supports_tls || ! listener_looks_like_azurite; then
    echo "azurite did not become ready on ${AZURITE_HOST}:${AZURITE_PORT}; see ${LOG_DIR}/azurite.log" >&2
    exit 1
  fi
fi

BASELINE_COMMIT="$(git -C "${ROOT_DIR}" rev-parse --verify "${BASELINE_REF}^{commit}" 2>/dev/null || true)"
if [[ -z "${BASELINE_COMMIT}" ]]; then
  git -C "${ROOT_DIR}" fetch --tags origin >/dev/null 2>&1 || true
  BASELINE_COMMIT="$(git -C "${ROOT_DIR}" rev-parse --verify "${BASELINE_REF}^{commit}" 2>/dev/null || true)"
fi
if [[ -z "${BASELINE_COMMIT}" ]]; then
  echo "failed to resolve baseline ref '${BASELINE_REF}'; set DJ_COMPARE_BASELINE_REF to a local ref or commit SHA" >&2
  exit 1
fi

git -C "${ROOT_DIR}" worktree add --detach "${WORKTREE_DIR}" "${BASELINE_COMMIT}" >/dev/null

mkdir -p "${WORKTREE_DIR}/pkg/store/storetest" "${WORKTREE_DIR}/pkg/store/filestore" "${WORKTREE_DIR}/pkg/store/adapter"
cp "${ROOT_DIR}/pkg/store/storetest/prodlike_workload.go" "${WORKTREE_DIR}/pkg/store/storetest/prodlike_workload.go"
cp "${ROOT_DIR}/scripts/prodlike_compare_baseline/filestore_compare_test.go.tmpl" "${WORKTREE_DIR}/pkg/store/filestore/prodlike_compare_test.go"
cp "${ROOT_DIR}/scripts/prodlike_compare_baseline/adapter_compare_test.go.tmpl" "${WORKTREE_DIR}/pkg/store/adapter/prodlike_compare_test.go"

echo "== current filestore =="
(
  cd "${ROOT_DIR}"
  go test ./pkg/store/filestore -run TestFileStoreProdLikeCompareHarness -count=1 -v | tee "${LOG_DIR}/current-filestore.log"
)

echo "== current objectstore (azurite) =="
(
  cd "${ROOT_DIR}"
  go test ./pkg/store/objectstore -run TestObjstoreProdLikeCompareHarness -count=1 -v | tee "${LOG_DIR}/current-objectstore-azure.log"
)

echo "== v0.3.10 filestore =="
(
  cd "${WORKTREE_DIR}"
  go test ./pkg/store/filestore -run TestFileStoreProdLikeCompareHarness -count=1 -v | tee "${LOG_DIR}/v0310-filestore.log"
)

echo "== v0.3.10 objectstore adapter (azurite) =="
(
  cd "${WORKTREE_DIR}"
  go test ./pkg/store/adapter -run TestObjstoreProdLikeCompareHarness -count=1 -v | tee "${LOG_DIR}/v0310-objectstore-azure.log"
)

echo
if [[ "${KEEP_ARTIFACTS}" == "1" ]]; then
  echo "logs written to ${LOG_DIR}"
else
  echo "set KEEP_ARTIFACTS=1 to retain logs under ${LOG_DIR}"
fi
