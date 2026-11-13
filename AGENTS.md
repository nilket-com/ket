# Agent instructions

Read "Test concurrency" in `ci/README.md` before classifying any test failure or
planning test runs. A test that fails only in parallel is a defect candidate,
never a flake; the repository already has dedicated lanes (`scripts/cargo-ctrl`,
`ci/controlled-tests.txt`, `nilket/.config/nextest.toml`,
`nilket/scripts/cargo-cgroup`).

Read "Daemon stacks and the real-process test" in `ci/README.md` before
delivering a change to the daemon's runtime, startup, transport or request
path. The in-process suite runs on 64 MiB stacks and cannot show a daemon
worker's stack overflow; the real three-daemon test must be run and recorded.
