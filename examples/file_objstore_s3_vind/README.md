# Local `FileStore -> objectstore(S3)` Example via `vcluster` Docker Driver

This example runs the ordered-tier `FileStore -> objectstore` scenario against a
local S3-compatible emulator instead of a real remote bucket.

- runtime: `vcluster` with the Docker driver
- emulator: `RustFS`
- cache shape: tier 0 `FileStore`, tier 1 `objectstore` backed by an S3 bucket

Use this for a fully local smoke test of the S3-backed objectstore path.

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
- the first request persists at least two remote objects into the S3 emulator
- deleting tier 0 removes the local files while tier 1 still keeps remote data
- the final request promotes data back into tier 0 after the wipe

The script will:

1. create or reuse a local `vcluster` on the Docker driver
2. deploy `RustFS`
3. port-forward the S3 endpoint to `127.0.0.1:9000`
4. run `go run .` with the right environment variables

## Manual Run

Create or reuse a local cluster and export a kubeconfig:

```bash
vcluster create daramjwee-local --namespace daramjwee-local --driver docker --connect=false --background-proxy=false
vcluster connect daramjwee-local --namespace daramjwee-local --driver docker --print >/tmp/daramjwee-s3.kubeconfig
```

Deploy and port-forward the emulator:

```bash
kubectl --kubeconfig /tmp/daramjwee-s3.kubeconfig apply -f manifests
kubectl --kubeconfig /tmp/daramjwee-s3.kubeconfig rollout status deployment/rustfs --timeout=180s
kubectl --kubeconfig /tmp/daramjwee-s3.kubeconfig port-forward service/rustfs 9000:9000
```

In another shell, run the example:

```bash
export DARAMJWEE_S3_ENDPOINT=127.0.0.1:9000
export DARAMJWEE_S3_BUCKET=daramjwee-s3-local
export DARAMJWEE_S3_REGION=us-east-1
export DARAMJWEE_S3_ACCESS_KEY=rustfsadmin
export DARAMJWEE_S3_SECRET_KEY=ChangeMe123!
go run .
```

## Environment

- `DARAMJWEE_S3_ENDPOINT`
  Default: `127.0.0.1:9000`
- `DARAMJWEE_S3_BUCKET`
  Default: `daramjwee-s3-local`
- `DARAMJWEE_S3_REGION`
  Default: `us-east-1`
- `DARAMJWEE_S3_ACCESS_KEY`
  Default: `rustfsadmin`
- `DARAMJWEE_S3_SECRET_KEY`
  Default: `ChangeMe123!`
