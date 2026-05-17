#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FUZZTIME="${DJ_LIVENESS_FUZZTIME:-10s}"
TIMEOUT="${DJ_LIVENESS_TIMEOUT:-1m}"
RUN_RACE="${DJ_LIVENESS_RACE:-0}"

SEED_PATTERN='^(TestCache_.*(BuiltInStagingStores|ContextAwareStagingStores)|FuzzCacheTopWriteLiveness)$'
FUZZ_PATTERN='^FuzzCacheTopWriteLiveness$'

echo "== cache top-write liveness seed tests =="
(
  cd "${ROOT_DIR}"
  go test ./tests -run "${SEED_PATTERN}" -count=1 -timeout="${TIMEOUT}"
)

if [[ "${RUN_RACE}" == "1" ]]; then
  echo
  echo "== cache top-write liveness seed tests (-race) =="
  (
    cd "${ROOT_DIR}"
    go test -race ./tests -run "${SEED_PATTERN}" -count=1 -timeout="${TIMEOUT}"
  )
fi

echo
echo "== cache top-write liveness fuzz (fuzztime=${FUZZTIME}) =="
(
  cd "${ROOT_DIR}"
  go test ./tests -run '^$' -fuzz "${FUZZ_PATTERN}" -fuzztime="${FUZZTIME}" -timeout="${TIMEOUT}"
)
