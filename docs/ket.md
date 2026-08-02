# ket product router

ket is the transparent front door for the product binaries. Its factory
default is Baryon:

    ket                         # Baryon unless configured otherwise
    ket baryon [ARGS...]
    ket polariton [ARGS...]
    ket nilket [ARGS...]
    ket toron [ARGS...]         # routes through nilket toron
    ket wendelon [ARGS...]      # routes through nilket wendelon

The bare default is visible and typed:

    ket default
    ket default baryon
    ket default polariton
    ket default nilket
    ket default reset

Toron and Wendelon are expert roles, not complete applications or deployments,
so they cannot become the bare default.

## Install from this checkout

The router and its targets are independently installed. Install only the
surfaces you use:

    cargo install --locked --force --path crates/ket
    cargo install --locked --force --path crates/baryon
    cargo install --locked --force --path crates/polariton
    CARGO_TARGET_DIR="$PWD/nilket/target" \
      cargo install --locked --force --path nilket/crates/nilket

The Nilket installation supplies the deployment and both expert role routes;
there are no separate Toron or Wendelon installation requirements for ket.
Polariton includes Toron support by default for the ordinary dogfood product;
use `--no-default-features` only when deliberately building the reduced head.

Resolution checks for a sibling executable beside ket, then searches PATH. A
missing target is an honest refusal with an install hint. The router does not
search Cargo target directories, auto-start Nilket for Polariton, or remain as
a supervising process.

V1 supports Linux, where ket uses process replacement so the target inherits
the same PID, signals, working directory, environment, and standard streams.
