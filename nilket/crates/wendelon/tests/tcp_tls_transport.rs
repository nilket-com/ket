#[path = "support/tls_pki.rs"]
mod tls_pki;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;
use wendelon::network::{
	TcpTlsTransportProvider, TlsMaterial, TransportProvider, TransportTier,
	canonical_server_name_resolver, rustls_client_config, rustls_server_config,
	verified_transport_peer_from_chain,
};
use wendelon::schema::NodeId;

fn fresh_dir(tag: &str) -> std::path::PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-tcp-tls-transport-{tag}-{}-{}",
		std::process::id(),
		Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

#[tokio::test]
async fn tcp_tls_provider_dials_and_redials_streams() {
	let root = fresh_dir("redial");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let client_material =
		TlsMaterial::load(&pki.data_client_cert, &pki.data_client_key, &pki.ca_cert).unwrap();
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let local_addr = listener.local_addr().unwrap();
	let acceptor = TlsAcceptor::from(Arc::new(rustls_server_config(&server_material).unwrap()));

	let server = tokio::spawn(async move {
		let mut peers = Vec::new();
		let mut payloads = Vec::new();
		for _ in 0..2 {
			let (tcp, _) = listener.accept().await.unwrap();
			let mut tls = acceptor.accept(tcp).await.unwrap();
			let certs = tls.get_ref().1.peer_certificates().unwrap();
			let peer = verified_transport_peer_from_chain(certs, tls_pki::CLUSTER_DOMAIN).unwrap();
			let mut body = Vec::new();
			tls.read_to_end(&mut body).await.unwrap();
			peers.push(peer.node_id.id);
			payloads.push(body);
		}
		(peers, payloads)
	});

	let provider = TcpTlsTransportProvider::with_cluster_domain(
		rustls_client_config(&client_material).unwrap(),
		canonical_server_name_resolver(tls_pki::CLUSTER_DOMAIN),
		tls_pki::CLUSTER_DOMAIN,
	);
	let target = NodeId {
		id: 1,
		_reserved: 0,
	};
	let connection = provider
		.connect(target, &local_addr.to_string())
		.await
		.unwrap();
	assert_eq!(connection.remote_id(), target);
	assert_eq!(connection.transport_type(), TransportTier::TcpTls);
	assert_eq!(
		connection.actor_identity().node_id.map(|node| node.id),
		Some(1)
	);

	let mut first = connection.open_bi().await.unwrap();
	first.write_all(b"first").await.unwrap();
	first.finish().await.unwrap();

	let mut second = connection.open_bi().await.unwrap();
	second.write_all(b"second").await.unwrap();
	second.finish().await.unwrap();

	let (peers, payloads) = server.await.unwrap();
	assert_eq!(peers, vec![2, 2]);
	assert_eq!(payloads, vec![b"first".to_vec(), b"second".to_vec()]);
}
