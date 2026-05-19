use std::io::{Error, ErrorKind};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Once};
use std::task::{Context, Poll};

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{
	ClientConfig, DigitallySignedStruct, Error as RustlsError, ServerConfig, SignatureScheme,
};
use rustls::{client::danger::HandshakeSignatureValid, crypto::ring};
use rustls::{client::danger::ServerCertVerified, client::danger::ServerCertVerifier};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use wendelon::network::quic_transport::QuicTransportStream;
use wendelon::network::transport::TransportStream;

const LOOPBACK_QUIC_ALPN: &[u8] = b"sql-task-loopback";
const LOOPBACK_QUIC_SERVER_NAME: &str = "sql-task-loopback.local";
static INSTALL_RUSTLS_PROVIDER: Once = Once::new();

#[derive(Clone)]
pub struct QuicEndpoint {
	inner: quinn::Endpoint,
}

impl QuicEndpoint {
	pub fn bind(bind_addr: SocketAddr) -> std::io::Result<Self> {
		let server_config = loopback_server_config()?;
		Ok(Self {
			inner: quinn::Endpoint::server(server_config, bind_addr)?,
		})
	}

	pub fn client() -> std::io::Result<Self> {
		let mut endpoint =
			quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
		endpoint.set_default_client_config(loopback_client_config()?);
		Ok(Self { inner: endpoint })
	}

	pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
		self.inner.local_addr()
	}

	pub async fn accept_stream(&self) -> std::io::Result<QuicStream> {
		let connection = self.accept_connection().await?;
		Self::accept_stream_on(&connection).await
	}

	pub async fn accept_connection(&self) -> std::io::Result<quinn::Connection> {
		let incoming = self
			.inner
			.accept()
			.await
			.ok_or_else(|| Error::new(ErrorKind::ConnectionAborted, "QUIC endpoint closed"))?;
		incoming
			.await
			.map_err(|error| Error::new(ErrorKind::ConnectionAborted, error))
	}

	pub async fn accept_stream_on(connection: &quinn::Connection) -> std::io::Result<QuicStream> {
		let (send, recv) = connection
			.accept_bi()
			.await
			.map_err(|error| Error::new(ErrorKind::ConnectionAborted, error))?;
		Ok(QuicStream::new(connection.clone(), send, recv))
	}

	pub async fn connect_stream(&self, remote: SocketAddr) -> std::io::Result<QuicStream> {
		let connection = self
			.inner
			.connect(remote, LOOPBACK_QUIC_SERVER_NAME)
			.map_err(|error| Error::new(ErrorKind::InvalidInput, error))?
			.await
			.map_err(|error| Error::new(ErrorKind::ConnectionRefused, error))?;
		let (send, recv) = connection
			.open_bi()
			.await
			.map_err(|error| Error::new(ErrorKind::ConnectionRefused, error))?;
		Ok(QuicStream::new(connection, send, recv))
	}

	pub fn close(&self) {
		self.inner.close(0u32.into(), b"shutdown");
	}
}

pub struct QuicStream {
	_connection: quinn::Connection,
	inner: QuicTransportStream,
}

impl QuicStream {
	fn new(
		connection: quinn::Connection,
		send: quinn::SendStream,
		recv: quinn::RecvStream,
	) -> Self {
		Self {
			_connection: connection,
			inner: QuicTransportStream::new(send, recv),
		}
	}

	pub async fn finish(&mut self) -> std::io::Result<()> {
		self.inner.finish().await
	}
}

impl AsyncRead for QuicStream {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		Pin::new(&mut this.inner).poll_read(cx, buf)
	}
}

impl AsyncWrite for QuicStream {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		let this = self.get_mut();
		Pin::new(&mut this.inner).poll_write(cx, buf)
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		Pin::new(&mut this.inner).poll_flush(cx)
	}

	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		Pin::new(&mut this.inner).poll_shutdown(cx)
	}
}

pub trait QuicIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> QuicIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

fn loopback_server_config() -> std::io::Result<quinn::ServerConfig> {
	install_rustls_provider();
	let cert = rcgen::generate_simple_self_signed(vec![LOOPBACK_QUIC_SERVER_NAME.to_string()])
		.map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
	let cert_der = CertificateDer::from(
		cert.serialize_der()
			.map_err(|error| Error::new(ErrorKind::InvalidInput, error))?,
	);
	let key_der = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.serialize_private_key_der()));
	let mut rustls_config = ServerConfig::builder()
		.with_no_client_auth()
		.with_single_cert(vec![cert_der], key_der)
		.map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
	rustls_config.alpn_protocols = vec![LOOPBACK_QUIC_ALPN.to_vec()];
	let quic_config = QuicServerConfig::try_from(rustls_config).map_err(|error| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("QUIC server config: {error}"),
		)
	})?;
	Ok(quinn::ServerConfig::with_crypto(Arc::new(quic_config)))
}

fn loopback_client_config() -> std::io::Result<quinn::ClientConfig> {
	install_rustls_provider();
	let mut rustls_config = ClientConfig::builder()
		.dangerous()
		.with_custom_certificate_verifier(Arc::new(LoopbackServerVerifier))
		.with_no_client_auth();
	rustls_config.alpn_protocols = vec![LOOPBACK_QUIC_ALPN.to_vec()];
	let quic_config = QuicClientConfig::try_from(rustls_config).map_err(|error| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("QUIC client config: {error}"),
		)
	})?;
	Ok(quinn::ClientConfig::new(Arc::new(quic_config)))
}

fn install_rustls_provider() {
	INSTALL_RUSTLS_PROVIDER.call_once(|| {
		let _ = ring::default_provider().install_default();
	});
}

#[derive(Debug)]
struct LoopbackServerVerifier;

impl ServerCertVerifier for LoopbackServerVerifier {
	fn verify_server_cert(
		&self,
		_end_entity: &CertificateDer<'_>,
		_intermediates: &[CertificateDer<'_>],
		_server_name: &ServerName<'_>,
		_ocsp_response: &[u8],
		_now: UnixTime,
	) -> Result<ServerCertVerified, RustlsError> {
		Ok(ServerCertVerified::assertion())
	}

	fn verify_tls12_signature(
		&self,
		_message: &[u8],
		_cert: &CertificateDer<'_>,
		_dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, RustlsError> {
		Ok(HandshakeSignatureValid::assertion())
	}

	fn verify_tls13_signature(
		&self,
		_message: &[u8],
		_cert: &CertificateDer<'_>,
		_dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, RustlsError> {
		Ok(HandshakeSignatureValid::assertion())
	}

	fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
		vec![
			SignatureScheme::ECDSA_NISTP256_SHA256,
			SignatureScheme::ECDSA_NISTP384_SHA384,
			SignatureScheme::ED25519,
			SignatureScheme::RSA_PSS_SHA256,
			SignatureScheme::RSA_PSS_SHA384,
			SignatureScheme::RSA_PSS_SHA512,
			SignatureScheme::RSA_PKCS1_SHA256,
			SignatureScheme::RSA_PKCS1_SHA384,
			SignatureScheme::RSA_PKCS1_SHA512,
		]
	}
}
