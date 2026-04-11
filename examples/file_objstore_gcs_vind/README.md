# Local `FileStore -> objectstore(GCS)` Example via `vcluster` Docker Driver

This example runs the existing ordered-tier `FileStore -> objectstore` scenario
against a local GCS emulator instead of real Google Cloud Storage.

- runtime: `vcluster` with the Docker driver
- emulator: `gcsemulator`
- cache shape: tier 0 `FileStore`, tier 1 `objectstore` backed by a GCS bucket

Use this when you want a fully local smoke test of the GCS-backed objectstore
path. If you want a real cloud bucket example instead, use
[`examples/file_objstore`](../file_objstore) or
[`examples/file_objstore_provider`](../file_objstore_provider).

## Prerequisites

- `docker`
- `go`
- `kubectl`
- `vcluster`

## Quick Run

```bash
./run.sh
```

## Verification Run

```bash
./verify.sh
```

The example is self-checking. It fails if any of these transitions do not
happen:

- the first request populates tier 0 with at least one local file
- the first request persists at least two remote objects into the GCS emulator
- deleting tier 0 removes the local files while tier 1 still keeps remote data
- the final request promotes data back into tier 0 after the wipe

The script will:

1. create or reuse a local `vcluster` on the Docker driver
2. deploy `gcsemulator`
3. port-forward the emulator to `127.0.0.1:4443`
4. run `go run .` with the right environment variables

## Manual Run

Create or reuse a local cluster and export a kubeconfig:

```bash
vcluster create daramjwee-local --namespace daramjwee-local --driver docker --connect=false --background-proxy=false
vcluster connect daramjwee-local --namespace daramjwee-local --driver docker --print >/tmp/daramjwee-gcs.kubeconfig
```

Deploy and port-forward the emulator:

```bash
kubectl --kubeconfig /tmp/daramjwee-gcs.kubeconfig apply -f manifests
kubectl --kubeconfig /tmp/daramjwee-gcs.kubeconfig rollout status deployment/gcsemulator --timeout=180s
kubectl --kubeconfig /tmp/daramjwee-gcs.kubeconfig port-forward service/gcsemulator 4443:9000
```

In another shell, run the example:

```bash
export DARAMJWEE_GCS_EMULATOR_HOST=127.0.0.1:4443
export DARAMJWEE_GCS_BUCKET=daramjwee-gcs-local
go run .
```

## Environment

- `DARAMJWEE_GCS_EMULATOR_HOST`
  Default: `127.0.0.1:4443`
- `DARAMJWEE_GCS_BUCKET`
  Default: `daramjwee-gcs-local`

The example uses `STORAGE_EMULATOR_HOST` internally so the Google Cloud Storage
client and the Thanos GCS provider both target the local emulator without a
real service account.
