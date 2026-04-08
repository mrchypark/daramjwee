#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COUNT="${DJ_RACE_COUNT:-20}"
TIMEOUT="${DJ_RACE_TIMEOUT:-5m}"

FILESTORE_PATTERN='^(TestFileStore_DeleteWaitsForPathLockBeforeUpdatingTracking|TestFileStore_EvictionDoesNotDropTrackingBeforeFileRemoval|TestFileStore_CloseReleasesEncodedLockBeforeLegacyCleanup|TestFileStore_BeginSetDoesNotHoldPathLockForWriterLifetime|TestFileStore_LateCloseDoesNotOverwriteNewerVisibleValue)$'
OBJECTSTORE_PATTERN='^(TestStore_BeginSetDoesNotHoldKeyLockForWriterLifetime|TestStore_LateCloseDoesNotOverwriteNewerVisibleValue|TestStore_LateCloseDoesNotResurrectDeletedKey|TestStore_OverwriteDefersPreviousSegmentRemovalUntilReaderCloses|TestStore_DeleteDefersSegmentRemovalUntilReaderCloses)$'

echo "== filestore lock contention race tests (count=${COUNT}) =="
(
  cd "${ROOT_DIR}"
  go test -race -count="${COUNT}" -timeout="${TIMEOUT}" ./pkg/store/filestore -run "${FILESTORE_PATTERN}"
)

echo
echo "== objectstore lock contention race tests (count=${COUNT}) =="
(
  cd "${ROOT_DIR}"
  go test -race -count="${COUNT}" -timeout="${TIMEOUT}" ./pkg/store/objectstore -run "${OBJECTSTORE_PATTERN}"
)

echo
echo "== optional race-tagged tests =="
(
  cd "${ROOT_DIR}"
  go test -race -tags=race -timeout="${TIMEOUT}" ./pkg/store/filestore ./pkg/store/objectstore
)
