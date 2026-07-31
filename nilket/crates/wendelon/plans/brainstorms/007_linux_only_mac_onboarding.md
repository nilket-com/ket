# Brainstorm 007: Linux-Only Architecture and the "Mac Problem"

## The Premise

Wendelon is being built to extract maximum performance from modern server hardware, utilizing Linux-specific APIs:
- **`io_uring`**: For zero-copy, asynchronous disk I/O.
- **RDMA (via `ibverbs` / `siw` / `rxe`)**: For microsecond-latency internal node-to-node replication.

The existential problem: The vast majority of early-adopter developers, open-source contributors, and "Hacker News" tire-kickers use macOS (Apple Silicon / aarch64). 

If a developer clones the Wendelon repository on a Mac and types `cargo build`, the compilation will explode due to missing Linux kernel headers, `liburing`, and `libibverbs`. If the onboarding experience is "spend two hours setting up a Linux VM," they will abandon the project immediately.

## The Temptation (And Why We Reject It)

The standard industry response is to build massive, leaky abstraction layers:
- Abstracting `io_uring` behind a trait that falls back to POSIX `pread/pwrite` thread pools or `kqueue` on macOS.
- Abstracting RDMA transport to fall back to standard TCP sockets for local Mac development.

**Verdict: Absolutely Not.**

Wendelon is a high-performance storage engine. Polluting the core architecture with OS-compatibility shims to support a laptop operating system destroys the codebase's purity and introduces branching logic that makes the Linux hot-paths harder to optimize and test.

Wendelon is **Linux Only. Forever.**

## The Solution: A World-Class Containerized Developer Experience

Instead of compromising the Rust code, we solve the "Mac Problem" entirely through developer tooling and environment virtualization.

### 1. The Graceful Rejection (`build.rs`)
If a user blindly types `cargo build` on macOS, we do not let it fail with a cryptic C-linker error. We catch it instantly.

We will add a `build.rs` script at the root of the workspace that checks `cfg!(target_os = "macos")` (or Windows). If true, the build aborts immediately with a friendly, opinionated error message:

> *"Wendelon is a high-performance database architected exclusively for Linux (`io_uring`, RDMA). To develop or run Wendelon on macOS, please use the provided Docker image or open the project using the included Devcontainer. See `docs/Mac_Setup.md`."*

This signals to serious engineers that we refuse to compromise our I/O architecture for laptop compatibility.

### 2. "Docker First" Evaluation
For users who just want to run Wendelon (not modify the code), the primary onboarding path is a pre-built Docker image. Docker Desktop (or OrbStack/Lima) runs a lightweight Linux VM on the Mac, providing the necessary kernel features.

```bash
# The official way to kick the tires on a Mac
docker run --privileged -p 8080:8080 wendelon/wendelon:latest
```
*(Note: `--privileged` may be required to load the software RDMA (`siw`) kernel modules inside the container.)*

### 3. Native-Feeling Contribution via Devcontainers
For Mac developers who want to write Rust code for Wendelon, they cannot use their local `rust-analyzer` because it will flag all Linux-specific code as errors.

We will commit a `.devcontainer/devcontainer.json` configuration to the repository.

When a contributor opens the repo in VS Code (or JetBrains/Cursor), they will be prompted to "Reopen in Container". The IDE will:
1. Spin up a Debian/Ubuntu Linux container.
2. Install the Rust toolchain, `liburing-dev`, and `rdma-core` inside the container.
3. Mount the Wendelon source code.
4. Connect the IDE's language server to the Linux environment.

To the Mac developer, it feels exactly like native development. Auto-complete, `cargo test`, and debugging all work flawlessly, but under the hood, they are executing against a real Linux kernel.

## Conclusion

We will not write a single line of macOS compatibility code in Wendelon's core. We will build a Ferrari for Linux, and explicitly provide the Docker "test track" for Mac users to drive it on. This preserves our architectural integrity while maintaining a frictionless onboarding experience.