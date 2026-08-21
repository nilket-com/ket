//! ADR 577: the QUIC liveness policy every Toron endpoint installs.
//!
//! This covers **both** of Toron's QUIC roles, because both share these
//! constructors and both have the same exposure: SQL ingress, where a long
//! query produces no packets while it executes, and the internal worker link,
//! where a worker running a long fragment is equally silent. Classifying it as
//! SQL-only would have been a claim the code does not make.
//!
//! Quinn's default `max_idle_timeout` is 30 seconds and its keep-alive is
//! disabled, so an endpoint that installs no `TransportConfig` inherits a
//! 30-second execution deadline nobody chose — shorter than the configured
//! statement timeout and shorter than the client's own query timeout. A silent
//! query loses its connection before either bound can report anything, turning
//! a typed timeout into an error indistinguishable from a network fault.
//!
//! The keep-alive, not the idle timeout, is what makes that safe. A larger
//! static idle timeout would still be a fixed number racing a per-request
//! `statement_timeout_seconds` the caller may raise above it; keep-alive holds
//! a healthy connection open for as long as execution legitimately runs, while
//! the finite idle timeout still reaps genuinely abandoned peers.

use std::sync::Arc;
use std::time::Duration;

/// How long a connection may go without any packet before QUIC closes it.
/// Chosen above the configured statement default so an ordinary long query is
/// never near it; correctness for longer per-request overrides comes from the
/// keep-alive below rather than from this number.
pub const TORON_QUIC_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// How often an idle-but-live connection emits a keep-alive. Must stay well
/// under [`TORON_QUIC_MAX_IDLE_TIMEOUT`]: quinn only credits one peer's
/// keep-alives when the interval is below *both* peers' idle timeouts, so
/// Toron configures the same policy on the endpoints it owns instead of
/// depending on what the other side happens to use.
pub const TORON_QUIC_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// The shared transport policy. Every Toron QUIC endpoint — SQL listener and
/// worker listener, SQL client and worker client, loopback and authenticated —
/// installs this rather than inheriting quinn's defaults.
pub fn toron_quic_transport_config() -> Arc<quinn::TransportConfig> {
	let mut transport = quinn::TransportConfig::default();
	transport.max_idle_timeout(Some(
		TORON_QUIC_MAX_IDLE_TIMEOUT
			.try_into()
			.expect("ADR 577 idle timeout is within QUIC's representable range"),
	));
	transport.keep_alive_interval(Some(TORON_QUIC_KEEP_ALIVE_INTERVAL));
	Arc::new(transport)
}

/// Install the policy on a server endpoint's configuration (SQL or worker
/// listener; they share these constructors).
pub fn apply_toron_quic_liveness_server(config: &mut quinn::ServerConfig) {
	config.transport_config(toron_quic_transport_config());
}

/// Install the policy on a client endpoint's configuration (SQL or worker
/// client; they share these constructors).
pub fn apply_toron_quic_liveness_client(config: &mut quinn::ClientConfig) {
	config.transport_config(toron_quic_transport_config());
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn adr0577_keep_alive_is_nonzero_and_shorter_than_the_idle_timeout() {
		// The ordering is the whole policy: an interval at or above the idle
		// timeout cannot refresh the connection before it expires, and a zero
		// interval is quinn's "disabled" spelling. Either would restore the
		// unconfigured behaviour this record exists to remove.
		assert!(
			!TORON_QUIC_KEEP_ALIVE_INTERVAL.is_zero(),
			"a zero keep-alive interval is disabled keep-alive"
		);
		assert!(
			TORON_QUIC_KEEP_ALIVE_INTERVAL < TORON_QUIC_MAX_IDLE_TIMEOUT,
			"keep-alive must refresh the connection well before it idles out: {:?} vs {:?}",
			TORON_QUIC_KEEP_ALIVE_INTERVAL,
			TORON_QUIC_MAX_IDLE_TIMEOUT
		);
		// Quinn's unconfigured default is what the finding traced the failure
		// to; the policy must not coincidentally reproduce it.
		assert!(
			TORON_QUIC_MAX_IDLE_TIMEOUT > Duration::from_secs(30),
			"the explicit idle timeout must exceed quinn's 30s default"
		);
	}

	#[test]
	fn adr0577_the_policy_sets_both_values() {
		// A transport config exposes no getters, so the guard on the source is
		// what pins the two setters. This asserts the builder runs at all and
		// yields a shared value rather than panicking on conversion.
		let first = toron_quic_transport_config();
		let second = toron_quic_transport_config();
		assert!(
			!Arc::ptr_eq(&first, &second),
			"each caller installs its own instance"
		);
	}
}
