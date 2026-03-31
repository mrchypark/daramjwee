# Prod-like Store Benchmark Design

> Benchmark design for comparing `filestore` and `objectstore` against `v0.3.10`.

**Goal:** Keep a reproducible prod-like benchmark in the repository that measures current `filestore`, current `objectstore + Azurite`, and the equivalent `v0.3.10` implementations with the same workload.

**Scope**

- Model the observed production-heavy cache shape rather than a synthetic single-key micro-benchmark.
- Preserve the metrics requested during analysis:
  - speed
  - internet usage
  - write count
  - read count
  - list call count
- Keep the benchmark out of default CI/test flows unless explicitly enabled.

**Workload**

- `292` logical keys
- Total payload: `11,565,616` bytes
- Composition:
  - `240 x 77B` negative-like `metrics/device/panel/15m`
  - `16 x 800B` plant metadata entries
  - `8 x 32KiB` registry entries
  - `8 x 96KiB` blueprint entries
  - `12 x 128KiB` medium metrics entries
  - `6 x 768KiB` large packed metrics entries
  - `2 x 2MiB` large direct metrics entries

**Design**

1. Add env-gated compare harnesses to `pkg/store/filestore` and `pkg/store/objectstore`.
2. Keep `filestore` measurement local-only and treat reads/writes as logical file operations.
3. Keep `objectstore` measurement on Azurite and instrument bucket operations so remote bytes and call counts are observable.
4. Add a runner script that creates a temporary `v0.3.10` worktree, injects version-appropriate harness files, runs both versions, and collects logs.
5. Document commands and the meaning of each metric in package READMEs.

**Trade-offs**

- Tests are used instead of `Benchmark...` because the comparison needs structured multi-phase output rather than `testing.B` throughput numbers.
- The harnesses are intentionally skipped unless `DJ_RUN_PRODLIKE_COMPARE=1` to avoid accidental execution in normal test runs.
- The `v0.3.10` benchmark files are generated temporarily by the runner script so the current branch remains the only maintained source.
