use rustls::pki_types::CertificateDer;
use std::io::Cursor;
use wendelon::network::quic_transport::canonical_server_name_resolver;
use wendelon::network::tls::{
	canonical_node_dns_name, verified_peer_node_id_from_chain, DEFAULT_CLUSTER_DOMAIN,
};
use wendelon::schema::NodeId;

const HAPPY_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIC8TCCAdmgAwIBAgIUBC4sk88Hn80ERuQRwfkP9wbdRmwwDQYJKoZIhvcNAQEL
BQAwEDEOMAwGA1UEAwwFaGFwcHkwHhcNMjYwNDE2MDEzNjU0WhcNMjYwNDE3MDEz
NjU0WjAQMQ4wDAYDVQQDDAVoYXBweTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBANcyQrv0l1DbnqRIZcfj9AWJbQ1x3rkxqE9R1F1bx+BbfPNnCKzmqTYg
6mBvWDVGuqFqmRTvaPDbaEGqCCzeObnyx+a1nQshRftbM42hzQdSasNf2LBJWxye
HsRUCyMZ+nUayLbGemCRGsWFhI4PmVm3Lai8HoRJcpbU/HdIe6GdBmQXFtzIFHbH
MieXIU0sLKCpVpaAi8nRVM3OJAVfOiapJUBs4l/4aQEUbgkGCHqw0hBEYzrt7/sC
AXZzVVFplt+kVyF+VWYTU91fjjbpqpwCEaxDdC7eHIDhq95wBfP2LsfUueb5w+w6
NW/LbEhmqvJjTsaABtZrLxw/NZwMUx8CAwEAAaNDMEEwIAYDVR0RBBkwF4IVbm9k
ZS00Mi5jbHVzdGVyLmxvY2FsMB0GA1UdDgQWBBTjRR/Vy5dilApE4W4IKAfu4AkF
nDANBgkqhkiG9w0BAQsFAAOCAQEAOna8ggsJ0icbAtuXQq14tjFQILcRCi2gK4B3
488w7M4XsrM+sggYOCb7LjNUzjJNPUDvm8uMS18Q80JF38IZqj4MNbxDG/tz0Z0F
BrxjeDZg3Xbo67SL7uHdINB4Uf3CdEBVQ6YyU5WxRWMGhHI/7mOn6O2qohNMtR+O
YZCLfWurRDaBRSqbYcyc/TofLMhlN91DOshKDNxyld+y4/13QIufyas/tDqGBgRu
wrUiLkFkD49FZm9CbAdjk2WjTJu510HjKD6Hv+gB5Fbg4on/D9gAVZgL3/nNDz27
j6RUM4Xxffjh2Q7nfFd2EAaS44t+HutEkewnrniayAAULxvd+g==
-----END CERTIFICATE-----
"#;

const MISMATCH_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIC9TCCAd2gAwIBAgIUFna5O+7smMZOEyOsg3AMpkRL9xEwDQYJKoZIhvcNAQEL
BQAwEzERMA8GA1UEAwwIbWlzbWF0Y2gwHhcNMjYwNDE2MDEzNjU0WhcNMjYwNDE3
MDEzNjU0WjATMREwDwYDVQQDDAhtaXNtYXRjaDCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBAJy3aRy0fEweddKJJoJrKRNiCwg8YKGJ/4JCrAeOIOXAtFnt
5k/nfTplOj7s7T0O3fVvh0I8Q2en5D/5eavFcUJ0ZQ43MGDpRLT1kJ1Jwt12EMAX
sJJJZ7WjVKUflqe7qaldfBoXvwlVPEu3AIxW8jUSx5BpsM0hEPp1VJbPy/CiJog2
1Yx1Hq9pSrB7EcTWlLn4o/UHRlsplAioVoC0cBTuOGz+HV+82aM8KmN5Hllh3qjT
jG/66eK/+nEnJAD0pUT7oHInzJRswQQBv/n3FufwsI0FfaiWtVs+Hwu/ki/dIQYL
m2u+SJeZBxF5FRG6RRdygLeWCm++qTMI+rY5oNkCAwEAAaNBMD8wHgYDVR0RBBcw
FYITbm9kZS00Mi5vdGhlci5sb2NhbDAdBgNVHQ4EFgQUGfLCsSDS+VJrL79Hx7hQ
SnHQ+s0wDQYJKoZIhvcNAQELBQADggEBAI1Ore+h2qgsdnsabKfzyAnq3DWsKnVl
B+fGE0rMyMXUiblkBjgot9jV+Du7ly3q9XLnZORYUnWnC/JdHn8TOkM67epTnKlv
jIzO90L00OfTd7xF0J906f0jFmIcKm0iKBU7wzmisInw9Z7adodEzHgT2FBax4fc
FvgREjRfJJV1BSiSreklBm1QGJrenNT5zMwrqCg9tlmpaCyCH32eqcEmer/S/Krv
jISedSCCUxuCy6K8gZC+GxcmbiN3ZybjwLvCV2Vwhecqu0LJKkEOvZe6kaC+vonA
HBzRBGA+8ehLth7ynSw5nzyYwxswGUNZ6THArQeOvaXz6elcZgIJVVM=
-----END CERTIFICATE-----
"#;

const MISSING_SAN_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDBTCCAe2gAwIBAgIUGTf3tVeHe8itdKiQJAbFlfCc0V8wDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHbWlzc2luZzAeFw0yNjA0MTYwMTM2NTVaFw0yNjA0MTcw
MTM2NTVaMBIxEDAOBgNVBAMMB21pc3NpbmcwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQCkDiooHASjbUXg6lMYMxE9T7hnL8dC+Pxl0naPKTeiuhN7tpLS
8nRBOXelh11FTJxrml8kzhJKsYaDAFgrnEWQabljVEJKWlKzwDopzAkWiuK99r6V
+9XZP69gRpyRcmgtiFsj3aX9zVbQXPbMz3PTdpQwg6gBzrUJ4tae2xwc2XOYZrio
ytK/jCYqTXB+gme0ZfbvxWynL6jKVzZKcXJfz0HCGg7AxonSr7qxkQ+fMFTHbBiI
JUDpYb9tx2lxvrXihKfIFuIdXii7nKxKlUxDjbL8+jQ8xG8Qt/RLUeicNQ4v6FSa
4DAuxF1CA/uO5wtZKlU9Pz6+ekZntc7eaXzbAgMBAAGjUzBRMB0GA1UdDgQWBBSe
Yj2+7QVEREFDBGBOaSU2W9+p2TAfBgNVHSMEGDAWgBSeYj2+7QVEREFDBGBOaSU2
W9+p2TAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAiUFXdWS70
5h9CsfNHS9wSYjaMNx94KOiOKCuth++k1XtoA0VHaeA0qPWx/r2eJDEp4Crsqcp7
FT4xzbnYWHkm7sFJIEwLQreSKWEcEBxzcNzbJxmrf1fTm9U1SttA+keQ29kV/3F2
G+F4iGu4E74nwfszn3EC7pAAh8iG3Jbu8kl7CZY+WO0O0vnXHVu+UgUtFXGlxirg
cb0Jbw4qSBM8A1k05ZWSpinSA3OK6JxNzUdaepboDv3h+QdZTq+0gld3T41+N/Pe
FJo7aMMKiyghW7mqlfECf+yMCEvEDVfF209n0obFPRTLyUzEVoqsUxIHMGDwkxLY
M93ory2Bo+kB
-----END CERTIFICATE-----
"#;

const UNPARSEABLE_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIC/jCCAeagAwIBAgIUHEKLyodFzR0B4y7Y8b6wdDVQCnQwDQYJKoZIhvcNAQEL
BQAwFjEUMBIGA1UEAwwLdW5wYXJzZWFibGUwHhcNMjYwNDE2MDEzNjU0WhcNMjYw
NDE3MDEzNjU0WjAWMRQwEgYDVQQDDAt1bnBhcnNlYWJsZTCCASIwDQYJKoZIhvcN
AQEBBQADggEPADCCAQoCggEBAKqnKHNVjy5cM4Ga71yb0ppkr0h0I+8vkSY2gP/w
/wy9eDbtkB34TNulMTqyIyV0Uasor4AWi/8R79KV3IVZn5TxSzwqDbNHXJcjdhiL
z8l00JpWn2JvwbuuX2j30UqQcgOkePlYIXAGK7IeQYacDvWt/eUV7nVEWAkfU/oE
FX5qpyPJslYQMhG3sXBod2AmJW4x84fibEx+4JmLbbmtkZb/Mg1NNuHamfWZ4upF
PMIcRw+8dXGMQaP041GbQedNPAhVUOmEBD8aLLKkSg2Tu5R+TVPHjVDNAL1RdTjr
SpJDqgmnpLLvp0xoWdk4FRiBwJltuy2H/2l+fBNw+gNGPkUCAwEAAaNEMEIwIQYD
VR0RBBowGIIWbm9kZS1hYmMuY2x1c3Rlci5sb2NhbDAdBgNVHQ4EFgQUZAifUz8S
Bd0QNadasFOkvFiVYxUwDQYJKoZIhvcNAQELBQADggEBAAdfH5zPlKRWM++qKvXa
H6zvEFjHCYUrot5EK8QcbgLmGmXU+nf5EhAVEipvnsDpbaE6JLPB11rdU89PukUi
aSbjUNnx3GTQSElxlM9OlpR4EQAhufnMsE80JgMRv3HVDlUEibQ62QSkAs2lX4ur
DGO/auZtVWuVeMxr0sbtNwP09Td+EvfxCQ/ll7cPUYk+UuHVTGpYtokqTJ+JI632
8y7yfXiWoIhPEWV0XbhhjwLK8lLuZcSjgnwTnbfPF0CG1s0KFdORcvr8Hr99dwGJ
lx6k9bmJo56J997H9a/ajzP8XUnCHct/5+Lx2YPOBtyrpxXGXCfNOZeN8HkiVXID
mRw=
-----END CERTIFICATE-----
"#;

const MULTIPLE_CANONICAL_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDDDCCAfSgAwIBAgIUCa3Ci6QpYPM0no9IMF3+N/naHWMwDQYJKoZIhvcNAQEL
BQAwEzERMA8GA1UEAwwIbXVsdGlwbGUwHhcNMjYwNDE2MDEzNjU0WhcNMjYwNDE3
MDEzNjU0WjATMREwDwYDVQQDDAhtdWx0aXBsZTCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBAKvXJ0vsQEhPB+j1rnjbem+RyEyDqN8IegOj+9mgJkw4dWEM
3U5peX6TaBRw3jsg3/ARIHLsHavmmcBo2ujKwzmDNl14AQLgEpL7C1f74h5rIttx
PsPxori9QPp/NfBfTVjdpBX6fmDx/UOGZDKdWqV4x65QAqZTpTZ0jHLNIjkUwUhu
vyB4Hivj2hrpsv0idRVfWqlXS9tC6tQIOzBbQklDsKIxRMYxljEfLcQ20sV54hmM
pG0B426QXZ1fhjEZ5QSSk/0+ZPfoFcUlw8qoaVBuE78lcZ68iwV5yxw+fFMPaxZK
V1Zw/AiTw421LzhMoUuRqUEvD2Od+h4jRpdcA6UCAwEAAaNYMFYwNQYDVR0RBC4w
LIIUbm9kZS0xLmNsdXN0ZXIubG9jYWyCFG5vZGUtMi5jbHVzdGVyLmxvY2FsMB0G
A1UdDgQWBBRVBSS0DCVetH1Z34g+Z7s1NkGQwDANBgkqhkiG9w0BAQsFAAOCAQEA
fjJB3ig23CxbCL9bzV5oWiBQJs7VwiE/rgwJIJWn+85NvCd/1dOYhggQtkVtAQhO
ECtqj4Qr18/A904TTZKEM9HRGpWS4fwJmEcEVCPSStJseBf226HEuQhIvp3pt7Rx
LhQ5a9AX/uqmbH4cE9qo7MfXkCCVTdvdyrqN/Km5ptm1cWv4jdTVkrJqa6v7154v
BkHruKdUZ3lAm017vF9HVxGgHA+ZcPfqi2dU7mk7zNgXwrqNvbJ7tGxH+UytLixU
sI12bXhieFwRPPZ4Nhax98gVmd3Svli8wTth/CXZA3vUCqKgWycqGAyAcxdU4g+0
UrnHkx9cSjgeHb3F+Efc+w==
-----END CERTIFICATE-----
"#;

fn certs_from_pem(pem: &str) -> Vec<CertificateDer<'static>> {
	rustls_pemfile::certs(&mut Cursor::new(pem.as_bytes()))
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
}

#[test]
fn canonical_name_helpers_match_the_plan() {
	assert_eq!(
		canonical_node_dns_name(42, DEFAULT_CLUSTER_DOMAIN),
		"node-42.cluster.local"
	);
	let resolver = canonical_server_name_resolver(DEFAULT_CLUSTER_DOMAIN);
	assert_eq!(resolver(42), "node-42.cluster.local");
}

#[test]
fn happy_path_parses_verified_node_id() {
	let certs = certs_from_pem(HAPPY_CERT_PEM);
	assert_eq!(
		verified_peer_node_id_from_chain(&certs, DEFAULT_CLUSTER_DOMAIN).unwrap(),
		NodeId {
			id: 42,
			_reserved: 0,
		}
	);
}

#[test]
fn san_mismatch_is_rejected() {
	let certs = certs_from_pem(MISMATCH_CERT_PEM);
	let err = verified_peer_node_id_from_chain(&certs, DEFAULT_CLUSTER_DOMAIN).unwrap_err();
	assert!(err.to_string().contains("no canonical node SAN"));
}

#[test]
fn missing_san_is_rejected() {
	let certs = certs_from_pem(MISSING_SAN_CERT_PEM);
	let err = verified_peer_node_id_from_chain(&certs, DEFAULT_CLUSTER_DOMAIN).unwrap_err();
	assert!(err.to_string().contains("no DNS SANs"));
}

#[test]
fn unparseable_san_is_rejected() {
	let certs = certs_from_pem(UNPARSEABLE_CERT_PEM);
	let err = verified_peer_node_id_from_chain(&certs, DEFAULT_CLUSTER_DOMAIN).unwrap_err();
	assert!(err.to_string().contains("not numeric"));
}

#[test]
fn multiple_canonical_sans_are_rejected() {
	let certs = certs_from_pem(MULTIPLE_CANONICAL_CERT_PEM);
	let err = verified_peer_node_id_from_chain(&certs, DEFAULT_CLUSTER_DOMAIN).unwrap_err();
	assert!(err.to_string().contains("more than one canonical"));
}
