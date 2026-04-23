//! Certificate issuance policy surface for Workstream F.
//!
//! This module intentionally does not stand up an RPC/HTTP issuer.
//! It is the policy engine that defines which leaf classes Wendelon
//! may issue, which CA must sign them, and what claim payload each
//! issued leaf must carry.

use crate::audit::{AuditAuthority, AuditEvent, AuditOperation, AuditOutcome};
use crate::network::tls::{
	ACTOR_CLAIMS_OID_DOTTED, ActorIdentity, ActorRole, ActorTrustDomain, BreakGlassScope,
	canonical_node_dns_name, encode_actor_claims_cbor_v1,
};
use crate::schema::{NodeId, TenantId};
use std::io::{Error, ErrorKind};
use std::time::Duration;

pub const NODE_CERT_MAX_TTL: Duration = Duration::from_secs(24 * 60 * 60);
pub const OPERATOR_CERT_MAX_TTL: Duration = Duration::from_secs(8 * 60 * 60);
pub const OBSERVER_CERT_MAX_TTL: Duration = Duration::from_secs(24 * 60 * 60);
pub const BREAK_GLASS_CERT_MAX_TTL: Duration = Duration::from_secs(15 * 60);
pub const QUERY_SESSION_CERT_MAX_TTL: Duration = Duration::from_secs(15 * 60);
pub const BREAK_GLASS_REASON_MIN_LEN: usize = 16;
pub const BREAK_GLASS_REASON_MAX_LEN: usize = 1024;
pub const BREAK_GLASS_INCIDENT_TICKET_MIN_LEN: usize = 3;
pub const BREAK_GLASS_INCIDENT_TICKET_MAX_LEN: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertAuthorityKind {
	NodeCa,
	OperatorCa,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertIssueRequest {
	Node {
		node_id: NodeId,
		key_id: Vec<u8>,
		ttl: Duration,
	},
	Operator {
		role: ActorRole,
		key_id: Vec<u8>,
		ttl: Duration,
	},
	Observer {
		key_id: Vec<u8>,
		ttl: Duration,
	},
	BreakGlass {
		key_id: Vec<u8>,
		ttl: Duration,
		scope: BreakGlassScope,
		incident_ticket_id: String,
		reason: String,
	},
	QuerySession {
		tenant_id: TenantId,
		key_id: Vec<u8>,
		ttl: Duration,
	},
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedCertIssue {
	pub authority: CertAuthorityKind,
	pub trust_domain: ActorTrustDomain,
	pub role: ActorRole,
	pub subject_common_name: String,
	pub dns_sans: Vec<String>,
	pub scopes: Vec<String>,
	pub tenant_id: Option<String>,
	pub key_id: Vec<u8>,
	pub ttl: Duration,
	pub break_glass_metadata: Option<PreparedBreakGlassMetadata>,
}

impl PreparedCertIssue {
	pub fn actor_claims_cbor(&self) -> Vec<u8> {
		encode_actor_claims_cbor_v1(
			self.role,
			self.scopes.as_slice(),
			self.tenant_id.as_deref(),
			self.key_id.as_slice(),
		)
	}

	pub fn break_glass_audit_event(
		&self,
		node_id: NodeId,
		issuer: &ActorIdentity,
	) -> std::io::Result<AuditEvent> {
		let Some(metadata) = &self.break_glass_metadata else {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"break-glass audit event requires a prepared break-glass issue",
			));
		};
		Ok(AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::IdentityBreakGlassIssued,
			node_id,
			format!(
				"issued break-glass certificate with {} scope",
				metadata.scope
			),
		)
		.with_actor(issuer)
		.with_outcome(AuditOutcome::BreakGlass)
		.with_parameter("issued_role", self.role.as_str())
		.with_parameter("issued_scope", metadata.scope.as_str())
		.with_parameter("incident_ticket_id", metadata.incident_ticket_id.clone())
		.with_parameter("reason", metadata.reason.clone())
		.with_parameter("ttl_seconds", self.ttl.as_secs()))
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedBreakGlassMetadata {
	pub scope: BreakGlassScope,
	pub incident_ticket_id: String,
	pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertIssuancePolicy {
	cluster_domain: String,
}

impl CertIssuancePolicy {
	pub fn new(cluster_domain: impl Into<String>) -> Self {
		Self {
			cluster_domain: cluster_domain.into(),
		}
	}

	pub fn cluster_domain(&self) -> &str {
		self.cluster_domain.as_str()
	}

	pub fn actor_claims_oid_dotted(&self) -> &'static str {
		ACTOR_CLAIMS_OID_DOTTED
	}

	pub fn prepare_issue(&self, request: CertIssueRequest) -> std::io::Result<PreparedCertIssue> {
		match request {
			CertIssueRequest::Node {
				node_id,
				key_id,
				ttl,
			} => {
				enforce_ttl("node", ttl, NODE_CERT_MAX_TTL)?;
				require_key_id(&key_id)?;
				Ok(PreparedCertIssue {
					authority: CertAuthorityKind::NodeCa,
					trust_domain: ActorTrustDomain::Data,
					role: ActorRole::DataNode,
					subject_common_name: format!("node-{}", node_id.id),
					dns_sans: vec![canonical_node_dns_name(node_id.id, self.cluster_domain())],
					scopes: vec!["cluster".to_string()],
					tenant_id: None,
					key_id,
					ttl,
					break_glass_metadata: None,
				})
			}
			CertIssueRequest::Operator { role, key_id, ttl } => {
				enforce_ttl("operator", ttl, OPERATOR_CERT_MAX_TTL)?;
				require_key_id(&key_id)?;
				match role {
					ActorRole::Operator
					| ActorRole::TenantAdmin
					| ActorRole::ClusterAdmin
					| ActorRole::BackupAdmin
					| ActorRole::ComplianceAuditor => Ok(PreparedCertIssue {
						authority: CertAuthorityKind::OperatorCa,
						trust_domain: ActorTrustDomain::Ops,
						role,
						subject_common_name: role.as_str().replace('_', "-"),
						dns_sans: Vec::new(),
						scopes: vec!["cluster".to_string()],
						tenant_id: None,
						key_id,
						ttl,
						break_glass_metadata: None,
					}),
					other => Err(Error::new(
						ErrorKind::InvalidInput,
						format!(
							"cert-service operator issuance does not support role {:?}",
							other
						),
					)),
				}
			}
			CertIssueRequest::Observer { key_id, ttl } => {
				enforce_ttl("observer", ttl, OBSERVER_CERT_MAX_TTL)?;
				require_key_id(&key_id)?;
				Ok(PreparedCertIssue {
					authority: CertAuthorityKind::NodeCa,
					trust_domain: ActorTrustDomain::Data,
					role: ActorRole::ReadOnlyObserver,
					subject_common_name: "read-only-observer".to_string(),
					dns_sans: Vec::new(),
					scopes: vec!["cluster".to_string()],
					tenant_id: None,
					key_id,
					ttl,
					break_glass_metadata: None,
				})
			}
			CertIssueRequest::BreakGlass {
				key_id,
				ttl,
				scope,
				incident_ticket_id,
				reason,
			} => {
				enforce_ttl("break-glass", ttl, BREAK_GLASS_CERT_MAX_TTL)?;
				require_key_id(&key_id)?;
				let incident_ticket_id =
					validate_break_glass_incident_ticket(incident_ticket_id.as_str())?;
				let reason = validate_break_glass_reason(reason.as_str())?;
				Ok(PreparedCertIssue {
					authority: CertAuthorityKind::OperatorCa,
					trust_domain: ActorTrustDomain::Ops,
					role: ActorRole::BreakGlassAdmin,
					subject_common_name: "break-glass".to_string(),
					dns_sans: Vec::new(),
					scopes: vec![scope.as_str().to_string()],
					tenant_id: None,
					key_id,
					ttl,
					break_glass_metadata: Some(PreparedBreakGlassMetadata {
						scope,
						incident_ticket_id,
						reason,
					}),
				})
			}
			CertIssueRequest::QuerySession {
				tenant_id,
				key_id,
				ttl,
			} => {
				enforce_ttl("query_session", ttl, QUERY_SESSION_CERT_MAX_TTL)?;
				require_key_id(&key_id)?;
				Ok(PreparedCertIssue {
					authority: CertAuthorityKind::NodeCa,
					trust_domain: ActorTrustDomain::Data,
					role: ActorRole::QuerySession,
					subject_common_name: format!("query-session-tenant-{}", tenant_id.get()),
					dns_sans: Vec::new(),
					scopes: vec!["query_session".to_string()],
					tenant_id: Some(tenant_id.get().to_string()),
					key_id,
					ttl,
					break_glass_metadata: None,
				})
			}
		}
	}
}

fn enforce_ttl(label: &str, requested: Duration, max: Duration) -> std::io::Result<()> {
	if requested.is_zero() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("{label} cert ttl must be greater than zero"),
		));
	}
	if requested > max {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("{label} cert ttl exceeds baseline maximum"),
		));
	}
	Ok(())
}

fn require_key_id(key_id: &[u8]) -> std::io::Result<()> {
	if key_id.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"issued cert key_id must not be empty",
		));
	}
	Ok(())
}

fn validate_break_glass_incident_ticket(raw: &str) -> std::io::Result<String> {
	let ticket = raw.trim();
	if ticket.len() < BREAK_GLASS_INCIDENT_TICKET_MIN_LEN
		|| ticket.len() > BREAK_GLASS_INCIDENT_TICKET_MAX_LEN
	{
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"break-glass incident_ticket_id length is out of bounds",
		));
	}
	if !ticket
		.chars()
		.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '/' | '.' | '#'))
	{
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"break-glass incident_ticket_id contains unsupported characters",
		));
	}
	Ok(ticket.to_string())
}

fn validate_break_glass_reason(raw: &str) -> std::io::Result<String> {
	let reason = raw.trim();
	if reason.len() < BREAK_GLASS_REASON_MIN_LEN || reason.len() > BREAK_GLASS_REASON_MAX_LEN {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"break-glass reason length is out of bounds",
		));
	}
	if reason.chars().any(char::is_control) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"break-glass reason must not contain control characters",
		));
	}
	Ok(reason.to_string())
}

#[cfg(test)]
mod tests {
	use super::{
		BREAK_GLASS_CERT_MAX_TTL, CertAuthorityKind, CertIssuancePolicy, CertIssueRequest,
		NODE_CERT_MAX_TTL, OBSERVER_CERT_MAX_TTL, OPERATOR_CERT_MAX_TTL,
		QUERY_SESSION_CERT_MAX_TTL,
	};
	use crate::network::tls::{
		ActorIdentity, ActorRole, ActorTrustDomain, BreakGlassScope, DEFAULT_CLUSTER_DOMAIN,
		parse_actor_claims_cbor,
	};
	use crate::schema::{NodeId, TenantId};
	use std::time::Duration;

	#[test]
	fn node_issuance_uses_node_ca_and_canonical_san() {
		let service = CertIssuancePolicy::new(DEFAULT_CLUSTER_DOMAIN);
		let prepared = service
			.prepare_issue(CertIssueRequest::Node {
				node_id: NodeId {
					id: 7,
					_reserved: 0,
				},
				key_id: vec![1, 2, 3, 4],
				ttl: NODE_CERT_MAX_TTL,
			})
			.unwrap();
		assert_eq!(prepared.authority, CertAuthorityKind::NodeCa);
		assert_eq!(prepared.trust_domain, ActorTrustDomain::Data);
		assert_eq!(prepared.role, ActorRole::DataNode);
		assert_eq!(prepared.dns_sans, vec!["node-7.cluster.local".to_string()]);
	}

	#[test]
	fn operator_subroles_issue_under_ops_ca() {
		let service = CertIssuancePolicy::new(DEFAULT_CLUSTER_DOMAIN);
		for role in [
			ActorRole::Operator,
			ActorRole::TenantAdmin,
			ActorRole::ClusterAdmin,
			ActorRole::BackupAdmin,
			ActorRole::ComplianceAuditor,
		] {
			let prepared = service
				.prepare_issue(CertIssueRequest::Operator {
					role,
					key_id: vec![9, 8, 7, 6],
					ttl: OPERATOR_CERT_MAX_TTL,
				})
				.unwrap();
			assert_eq!(prepared.authority, CertAuthorityKind::OperatorCa);
			assert_eq!(prepared.trust_domain, ActorTrustDomain::Ops);
			assert_eq!(prepared.role, role);
			assert_eq!(prepared.scopes, vec!["cluster".to_string()]);
		}
	}

	#[test]
	fn query_session_is_tenant_bound_and_short_lived() {
		let service = CertIssuancePolicy::new(DEFAULT_CLUSTER_DOMAIN);
		let prepared = service
			.prepare_issue(CertIssueRequest::QuerySession {
				tenant_id: TenantId::new(42),
				key_id: vec![1, 2, 3, 4],
				ttl: QUERY_SESSION_CERT_MAX_TTL,
			})
			.unwrap();
		assert_eq!(prepared.authority, CertAuthorityKind::NodeCa);
		assert_eq!(prepared.trust_domain, ActorTrustDomain::Data);
		assert_eq!(prepared.role, ActorRole::QuerySession);
		assert_eq!(prepared.scopes, vec!["query_session".to_string()]);
		assert_eq!(prepared.tenant_id.as_deref(), Some("42"));
	}

	#[test]
	fn query_session_claims_roundtrip_through_frozen_parser() {
		let service = CertIssuancePolicy::new(DEFAULT_CLUSTER_DOMAIN);
		let prepared = service
			.prepare_issue(CertIssueRequest::QuerySession {
				tenant_id: TenantId::new(12),
				key_id: vec![5, 4, 3, 2],
				ttl: Duration::from_secs(60),
			})
			.unwrap();
		let decoded = parse_actor_claims_cbor(prepared.actor_claims_cbor().as_slice()).unwrap();
		assert_eq!(decoded.role, ActorRole::QuerySession);
		assert_eq!(decoded.scopes, vec!["query_session".to_string()]);
		assert_eq!(decoded.tenant_id.as_deref(), Some("12"));
		assert_eq!(decoded.key_id, vec![5, 4, 3, 2]);
	}

	#[test]
	fn break_glass_requires_reason_ticket_scope_and_short_ttl() {
		let service = CertIssuancePolicy::new(DEFAULT_CLUSTER_DOMAIN);
		let err = service
			.prepare_issue(CertIssueRequest::BreakGlass {
				key_id: vec![1, 1, 1, 1],
				ttl: BREAK_GLASS_CERT_MAX_TTL,
				scope: BreakGlassScope::Diagnostic,
				incident_ticket_id: "  ".to_string(),
				reason: "incident response only".to_string(),
			})
			.unwrap_err();
		assert!(err.to_string().contains("incident_ticket_id"));

		let err = service
			.prepare_issue(CertIssueRequest::BreakGlass {
				key_id: vec![1, 1, 1, 1],
				ttl: BREAK_GLASS_CERT_MAX_TTL,
				scope: BreakGlassScope::Diagnostic,
				incident_ticket_id: "INC-42".to_string(),
				reason: "short".to_string(),
			})
			.unwrap_err();
		assert!(err.to_string().contains("reason length"));

		let err = service
			.prepare_issue(CertIssueRequest::BreakGlass {
				key_id: vec![1, 1, 1, 1],
				ttl: BREAK_GLASS_CERT_MAX_TTL + Duration::from_secs(1),
				scope: BreakGlassScope::Diagnostic,
				incident_ticket_id: "INC-42".to_string(),
				reason: "disk corruption requires emergency diagnostics".to_string(),
			})
			.unwrap_err();
		assert!(err.to_string().contains("baseline maximum"));
	}

	#[test]
	fn break_glass_issue_is_single_scope_and_builds_audit_event() {
		let service = CertIssuancePolicy::new(DEFAULT_CLUSTER_DOMAIN);
		let prepared = service
			.prepare_issue(CertIssueRequest::BreakGlass {
				key_id: vec![1, 1, 1, 1],
				ttl: BREAK_GLASS_CERT_MAX_TTL,
				scope: BreakGlassScope::Diagnostic,
				incident_ticket_id: "INC-4242".to_string(),
				reason: "cluster is degraded and needs emergency observability access".to_string(),
			})
			.unwrap();
		assert_eq!(prepared.scopes, vec!["diagnostic".to_string()]);
		assert_eq!(
			prepared
				.break_glass_metadata
				.as_ref()
				.map(|metadata| metadata.scope),
			Some(BreakGlassScope::Diagnostic)
		);
		let issuer = ActorIdentity::new(
			ActorRole::Operator,
			None,
			None,
			vec!["cluster".to_string()],
			vec![9, 9, 9, 9],
		)
		.unwrap();
		let event = prepared
			.break_glass_audit_event(
				NodeId {
					id: 7,
					_reserved: 0,
				},
				&issuer,
			)
			.unwrap();
		assert_eq!(event.operation.as_str(), "identity.break_glass_issued");
		assert_eq!(event.outcome.as_str(), "break_glass");
		assert_eq!(
			event.parameters.get("incident_ticket_id").unwrap().as_str(),
			Some("INC-4242")
		);
		assert_eq!(
			event.parameters.get("issued_scope").unwrap().as_str(),
			Some("diagnostic")
		);
		assert_eq!(
			event.parameters.get("reason").unwrap().as_str(),
			Some("cluster is degraded and needs emergency observability access")
		);
	}

	#[test]
	fn baseline_rejects_general_tenant_application_ca_shape() {
		let service = CertIssuancePolicy::new(DEFAULT_CLUSTER_DOMAIN);
		let err = service
			.prepare_issue(CertIssueRequest::Operator {
				role: ActorRole::QuerySession,
				key_id: vec![2, 2, 2, 2],
				ttl: OPERATOR_CERT_MAX_TTL,
			})
			.unwrap_err();
		assert!(err.to_string().contains("does not support role"));
	}

	#[test]
	fn observer_issuance_stays_on_node_ca() {
		let service = CertIssuancePolicy::new(DEFAULT_CLUSTER_DOMAIN);
		let prepared = service
			.prepare_issue(CertIssueRequest::Observer {
				key_id: vec![3, 3, 3, 3],
				ttl: OBSERVER_CERT_MAX_TTL,
			})
			.unwrap();
		assert_eq!(prepared.authority, CertAuthorityKind::NodeCa);
		assert_eq!(prepared.role, ActorRole::ReadOnlyObserver);
	}
}
