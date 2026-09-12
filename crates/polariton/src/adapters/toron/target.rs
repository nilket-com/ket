//! Where a Toron connection goes and how it authenticates (ADR 0165).
//!
//! The four authenticated fields are one unit: all present selects the
//! authenticated transport, none selects the loopback transport, and any
//! other combination is refused naming what is missing (§4). Failures are
//! described by what this client can know: a server-name mismatch is a local
//! finding and says so; a rejection by the peer is reported as the peer's
//! rejection without a cause the peer did not disclose (§5).

use std::path::PathBuf;

use toron_client::{ClientError, ToronClientTls};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToronTls {
	pub server_name: String,
	pub cert_path: PathBuf,
	pub key_path: PathBuf,
	pub ca_path: PathBuf,
}

impl ToronTls {
	pub(crate) fn client_tls(&self) -> ToronClientTls {
		ToronClientTls {
			server_name: self.server_name.clone(),
			cert_path: self.cert_path.clone(),
			key_path: self.key_path.clone(),
			ca_path: self.ca_path.clone(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToronTarget {
	Loopback { endpoint: String },
	Authenticated { endpoint: String, tls: ToronTls },
}

impl ToronTarget {
	pub fn endpoint(&self) -> &str {
		match self {
			Self::Loopback { endpoint } | Self::Authenticated { endpoint, .. } => endpoint,
		}
	}
}

pub const TLS_FIELD_KEYS: [&str; 4] = ["server_name", "client_cert", "client_key", "ca_cert"];

/// Resolve the connection form's fields into a target.
pub fn target_from_fields(
	endpoint: &str,
	server_name: &str,
	client_cert: &str,
	client_key: &str,
	ca_cert: &str,
) -> Result<ToronTarget, String> {
	let endpoint = endpoint.trim();
	if endpoint.is_empty() {
		return Err("Toron connection needs an endpoint (host:port)".to_owned());
	}
	let values = [server_name, client_cert, client_key, ca_cert].map(str::trim);
	let missing: Vec<&str> = TLS_FIELD_KEYS
		.iter()
		.zip(values)
		.filter(|(_, value)| value.is_empty())
		.map(|(key, _)| *key)
		.collect();
	match missing.len() {
		4 => {
			// The loopback client carries no identity, so it may only reach a
			// loopback address; refusing here keeps a remote endpoint without
			// TLS from starting an attempt the transport would refuse anyway.
			if let Ok(address) = endpoint.parse::<std::net::SocketAddr>()
				&& !address.ip().is_loopback()
			{
				return Err(
					"Toron loopback client requires a loopback endpoint; use authenticated mTLS remotely"
						.to_owned(),
				);
			}
			Ok(ToronTarget::Loopback {
				endpoint: endpoint.to_owned(),
			})
		}
		0 => Ok(ToronTarget::Authenticated {
			endpoint: endpoint.to_owned(),
			tls: ToronTls {
				server_name: values[0].to_owned(),
				cert_path: PathBuf::from(values[1]),
				key_path: PathBuf::from(values[2]),
				ca_path: PathBuf::from(values[3]),
			},
		}),
		_ => Err(format!(
			"Toron authenticated connection needs all of server_name, client_cert, client_key, and ca_cert; missing: {}",
			missing.join(", ")
		)),
	}
}

/// Describe a failed connection attempt for the status line.
pub fn describe_connect_error(error: &ClientError, tls: Option<&ToronTls>) -> String {
	match error {
		ClientError::Configuration(message) if message.contains("is not private") => {
			format!("client key refused before connecting: {message}")
		}
		ClientError::Transport(message) => {
			// rustls names a hostname mismatch `NotValidForName` in Debug form and
			// "not valid for name" in Display form; that finding is this client's
			// own, so it can be stated precisely.
			if message.contains("NotValidForName") || message.contains("not valid for name") {
				return match tls {
					Some(tls) => format!(
						"server name mismatch: the listener's certificate does not cover {}",
						tls.server_name
					),
					None => "server name mismatch".to_owned(),
				};
			}
			// An unknown issuer or a failed signature means the chain did not
			// verify against the configured CA; which side is wrong is not
			// established, so the underlying finding is retained. Any other
			// peer-certificate finding (expiry, key usage, ...) is reported as
			// rustls stated it, with no cause added.
			if message.contains("UnknownIssuer")
				|| message.contains("unknown issuer")
				|| message.contains("BadSignature")
				|| message.contains("bad signature")
			{
				return format!(
					"the listener's certificate could not be verified against the configured CA ({message})"
				);
			}
			if message.contains("invalid peer certificate") {
				return format!("the listener's certificate was rejected ({message})");
			}
			// The connection ended during an authenticated handshake. The
			// listener may have rejected this client's certificate, or it may
			// be down or unreachable; nothing here establishes which, so the
			// report keeps that uncertainty.
			if tls.is_some()
				&& (message.contains("alert")
					|| message.contains("closed")
					|| message.contains("reset")
					|| message.contains("handshake")
					|| message.contains("connection lost"))
			{
				return format!(
					"the connection ended during the authenticated handshake ({message}); the listener may have rejected this client's certificate, or it may be unavailable"
				);
			}
			format!("Toron transport: {message}")
		}
		other => format!("Toron connection: {other}"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn all_four_fields_select_the_authenticated_transport() {
		let target = target_from_fields(
			"192.168.0.2:7742",
			"toron-1.cluster.local",
			"C:/pki/client.crt",
			"C:/pki/client.key",
			"C:/pki/ca.crt",
		)
		.unwrap();
		assert!(matches!(target, ToronTarget::Authenticated { .. }));
		assert_eq!(target.endpoint(), "192.168.0.2:7742");
	}

	#[test]
	fn no_tls_field_selects_loopback() {
		let target = target_from_fields("127.0.0.1:7762", "", " ", "", "").unwrap();
		assert_eq!(
			target,
			ToronTarget::Loopback {
				endpoint: "127.0.0.1:7762".to_owned()
			}
		);
		// A remote endpoint without an identity is refused before any transport.
		let error = target_from_fields("192.0.2.1:7742", "", "", "", "").unwrap_err();
		assert!(error.contains("loopback endpoint"), "{error}");
	}

	#[test]
	fn a_partial_configuration_is_refused_naming_the_missing_fields() {
		let error = target_from_fields("h:1", "name", "", "key", "").unwrap_err();
		assert!(error.contains("missing: client_cert, ca_cert"), "{error}");
		let error = target_from_fields("h:1", "", "", "", "ca").unwrap_err();
		assert!(
			error.contains("missing: server_name, client_cert, client_key"),
			"{error}"
		);
	}

	#[test]
	fn an_empty_endpoint_is_refused_first() {
		assert!(target_from_fields("", "", "", "", "").is_err());
	}

	fn tls() -> ToronTls {
		ToronTls {
			server_name: "toron-1.cluster.local".to_owned(),
			cert_path: "c".into(),
			key_path: "k".into(),
			ca_path: "a".into(),
		}
	}

	#[test]
	fn a_server_name_mismatch_is_reported_precisely() {
		let error = ClientError::Transport("invalid peer certificate: NotValidForName".to_owned());
		let text = describe_connect_error(&error, Some(&tls()));
		assert_eq!(
			text,
			"server name mismatch: the listener's certificate does not cover toron-1.cluster.local"
		);
	}

	#[test]
	fn a_peer_rejection_is_reported_without_an_invented_cause() {
		let error = ClientError::Transport("connection closed: peer sent alert".to_owned());
		let text = describe_connect_error(&error, Some(&tls()));
		assert!(
			text.starts_with("the connection ended during the authenticated handshake")
				&& text.contains("may have rejected"),
			"{text}"
		);
		assert!(!text.contains("untrusted"), "no cause is invented: {text}");
	}

	#[test]
	fn a_refused_key_is_reported_as_refused_before_connecting() {
		let error = ClientError::Configuration(
			"private key k is not private: mode 644; expected 600".to_owned(),
		);
		let text = describe_connect_error(&error, Some(&tls()));
		assert!(
			text.starts_with("client key refused before connecting"),
			"{text}"
		);
	}

	#[test]
	fn the_two_certificate_reports_are_distinct() {
		let mismatch = describe_connect_error(
			&ClientError::Transport("x NotValidForName".to_owned()),
			Some(&tls()),
		);
		let rejection = describe_connect_error(
			&ClientError::Transport("handshake failed: alert".to_owned()),
			Some(&tls()),
		);
		assert_ne!(mismatch, rejection);
	}

	/// ADR 0166 acceptance 1: a listener that drops the connection during an
	/// authenticated handshake is reported as the peer's doing, distinct from
	/// this client's own certificate findings.
	#[test]
	fn a_lost_authenticated_connection_keeps_its_uncertainty() {
		let text = describe_connect_error(
			&ClientError::Transport("connection lost".to_owned()),
			Some(&tls()),
		);
		assert!(
			text.starts_with("the connection ended during the authenticated handshake")
				&& text.contains("may have rejected")
				&& text.contains("may be unavailable"),
			"{text}"
		);
		let plain =
			describe_connect_error(&ClientError::Transport("connection lost".to_owned()), None);
		assert_eq!(plain, "Toron transport: connection lost");
	}

	/// ADR 0166 acceptance 1: quinn reports rustls errors in Display form,
	/// and the name mismatch and the untrusted issuer stay distinct there.
	#[test]
	fn display_form_certificate_errors_are_classified_distinctly() {
		let mismatch = describe_connect_error(
			&ClientError::Transport(
				"invalid peer certificate: certificate not valid for name \"toron-9.cluster.local\"; certificate is only valid for DNS:toron-1.cluster.local".to_owned(),
			),
			Some(&tls()),
		);
		assert!(mismatch.starts_with("server name mismatch"), "{mismatch}");
		let issuer = describe_connect_error(
			&ClientError::Transport("invalid peer certificate: unknown issuer".to_owned()),
			Some(&tls()),
		);
		assert!(
			issuer.starts_with(
				"the listener's certificate could not be verified against the configured CA"
			) && issuer.contains("unknown issuer"),
			"{issuer}"
		);
		// Another peer-certificate finding is not turned into a CA claim.
		let expired = describe_connect_error(
			&ClientError::Transport("invalid peer certificate: certificate has expired".to_owned()),
			Some(&tls()),
		);
		assert!(
			expired.starts_with("the listener's certificate was rejected")
				&& expired.contains("expired")
				&& !expired.contains("configured CA"),
			"{expired}"
		);
	}
}
