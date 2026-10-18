# Agent instructions

Read "Test concurrency" in `ci/README.md` before classifying any test failure or
planning test runs. A test that fails only in parallel is a defect candidate,
never a flake; the repository already has dedicated lanes (`scripts/cargo-ctrl`,
`ci/controlled-tests.txt`, `nilket/.config/nextest.toml`,
`nilket/scripts/cargo-cgroup`).
