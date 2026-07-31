# Elle Handoff

Wendelon does not link Elle, Clojure, Java, or JNI code into the Rust
workspace.

The simulator emits focused register-history artifacts for out-of-process
analysis when `WENDELON_SIM_HISTORY_DIR` is set. `ci/check_simulator_ladder.sh`
treats that path as a root and writes run-specific artifacts beneath it. The
current artifact format is JSON and includes:

- scenario and seed metadata
- bounded register-history batches
- a flattened invoke/ok event stream for post-processors

Typical local flow:

```bash
export WENDELON_SIM_HISTORY_DIR=/tmp/wendelon-elle
bash ci/check_simulator_ladder.sh 10000 424242
```

To hand artifacts to an external Elle runner, point `WENDELON_ELLE_RUNNER` at a
wrapper script or container entrypoint that accepts one JSON artifact path:

```bash
export WENDELON_SIM_HISTORY_DIR=/tmp/wendelon-elle
export WENDELON_ELLE_RUNNER=/path/to/elle-wrapper
bash ci/check_simulator_ladder.sh 10000 424242
```

`ci/run_elle_handoff.sh` only invokes that external runner. It does not add any
JVM tooling to Cargo or to the Wendelon test binaries.
