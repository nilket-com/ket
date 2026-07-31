use crate::error::PkiError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
	pub prefix: String,
	pub id_token: String,
	pub cluster_domain: String,
}

pub fn admit_san(
	san: &str,
	cluster_domain: &str,
	allowed_prefixes: &[&str],
) -> Result<PeerIdentity, PkiError> {
	let suffix = format!(".{cluster_domain}");
	let leading = san
		.strip_suffix(&suffix)
		.ok_or_else(|| PkiError::SanShapeRejected {
			reason: "missing cluster_domain suffix".to_string(),
		})?;
	if leading.contains('.') {
		return Err(PkiError::SanShapeRejected {
			reason: "subdomain injection".to_string(),
		});
	}
	let (prefix, id_token) = leading
		.split_once('-')
		.ok_or_else(|| PkiError::SanShapeRejected {
			reason: "missing prefix-id_token split".to_string(),
		})?;
	if prefix.is_empty()
		|| id_token.is_empty()
		|| !id_token
			.chars()
			.all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
	{
		return Err(PkiError::SanShapeRejected {
			reason: "id_token must be [A-Za-z0-9-]+".to_string(),
		});
	}
	if !allowed_prefixes.contains(&prefix) {
		return Err(PkiError::SanPrefixRejected {
			prefix: prefix.to_string(),
			allowed: allowed_prefixes
				.iter()
				.map(|prefix| prefix.to_string())
				.collect(),
		});
	}
	Ok(PeerIdentity {
		prefix: prefix.to_string(),
		id_token: id_token.to_string(),
		cluster_domain: cluster_domain.to_string(),
	})
}

pub fn canonical_dns_name(prefix: &str, id_token: &str, cluster_domain: &str) -> String {
	format!("{prefix}-{id_token}.{cluster_domain}")
}
