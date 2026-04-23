#[path = "support/tls_pki.rs"]
mod tls_pki;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use rustls::pki_types::{CertificateDer, pem::PemObject};
use wendelon::consensus::{RevokeActorKey, raft_storage::RevocationSnapshot};
use wendelon::network::quic_transport::canonical_server_name_resolver;
use wendelon::network::tls::{
	ActorIdentity, ActorPermission, ActorRole, ActorTrustDomain, ActorTrustRoots, BreakGlassScope,
	DEFAULT_CLUSTER_DOMAIN, authorize_actor, canonical_node_dns_name,
	verified_peer_actor_identity_from_chain_with_trust_roots,
	verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations,
	verified_peer_node_id_from_chain, verified_transport_peer_from_chain,
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

const DATA_NODE_CLAIMS_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDRTCCAi2gAwIBAgIULHvalFKOk/Zt3Fy2FvWycglp0pswDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJZGF0YS1ub2RlMB4XDTI2MDQxOTA3NTk1MloXDTI2MDQy
MDA3NTk1MlowFDESMBAGA1UEAwwJZGF0YS1ub2RlMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAqCi1yRonSFqC/8YoMjUyyIlHHR7MJo7x8osLGcoNVJgC
G6lMtIdhpa1XUPtXVYeO8pWQOwYL4HNTT1KYloxvQAnDnE/HXzLvdN2bsf0ZQ0jJ
z5bzY3qMROxBm6nNaqvGKZO0HHJaf3+WJtCTlBEkBvVOMbfVNi6o8ja1Ii7YtL1I
Vkidw5hi1s/L6aDNilyVeuHuckeDB3R/kxTftJtcjt0imUXELNEGQRNut3wpYzr/
cpknBpS+zN6KEgodf85n6CuQDn74M9jaI+bWLecPFULGlLDzDXVACDB9bTirLELK
/NhZ8aPL5RnLRCNtRV62EXtNdlA3BU8j3/7cedYEEQIDAQABo4GOMIGLMCAGA1Ud
EQQZMBeCFW5vZGUtNDIuY2x1c3Rlci5sb2NhbDBIBgorBgEEAYOyAwEBAQH/BDek
ZHJvbGVpZGF0YV9ub2RlZnNjb3Blc4FnY2x1c3Rlcml0ZW5hbnRfaWT2ZmtleV9p
ZEQBAgMEMB0GA1UdDgQWBBSPYYuU5ic/yR3W1ltXNP2XkHpH7TANBgkqhkiG9w0B
AQsFAAOCAQEAUvopm718Jz1Ilhq6LYm9+iqtwkDKiPcdhpfPujfqHKNDObVk7lnF
HbWJhJNgr19o2DgG0VSK0jbqY3rc+KV78K8Rr4P082tdiHbLNAvFLejtEnNb2ysj
w1svDOrRs1OTQdjyaMj2nLXVebEpsqt2UYb2FxujBfTRXBEtVc1lSF1VCvPgns4A
jAOvkA9qG+Nzeia+jmQhDmyMG8HWUv3ppkNjWvtDEa9K5MVZaVV19EK8pTgWL5yV
7xM7Q7FV8sTXm1oMLeuMw6chZPQoUt9o3VyRWtkTH2Q7KBv8nlSM7HbJ8yhcJ8v4
1pHIsHgjxNxkxXbgDLBE/7Snzo9xhB5M3w==
-----END CERTIFICATE-----
"#;

const OPERATOR_CLAIMS_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDQjCCAiqgAwIBAgIUV2Vvp6W+CrDCNWeG5ihPVSvm7mIwDQYJKoZIhvcNAQEL
BQAwEzERMA8GA1UEAwwIb3BlcmF0b3IwHhcNMjYwNDE5MDgwMDAyWhcNMjYwNDIw
MDgwMDAyWjATMREwDwYDVQQDDAhvcGVyYXRvcjCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBAKjvAgtnw9u9bVkZL+7gm/8gIhZZZhWc81IGM9AioOZKN7cT
EWvstIOZD2bUOWNMGiqkz1mK/thJsR/VG3DOQ+5AUw54jH084E5ZyshaAE3+tMr0
BFOkJnmS51ZRYhY3KZA7B2JrCdNWN/HopgVzmkr78PZezWRqOCwnRkhwmMNPdY3p
dZzj1vn4j/RYD1Qa5RLJ81CFXyA74w5qHFKsDn4WH2lCRBFC0PPoko1nIX/ZId6M
RyB6Gg2TYIpoXtnLPxGbuvgUdL2KShswy3Oh9slNPOWx1mJlPYad5UWz28GPTo7A
KXuW8kK/APmjsxWeREtfcPLGEPCRnKylCulj6AMCAwEAAaOBjTCBijAgBgNVHREE
GTAXghVub2RlLTQyLmNsdXN0ZXIubG9jYWwwRwYKKwYBBAGDsgMBAQEB/wQ2pGRy
b2xlaG9wZXJhdG9yZnNjb3Blc4FnY2x1c3Rlcml0ZW5hbnRfaWT2ZmtleV9pZEQJ
CAcGMB0GA1UdDgQWBBQYeZlSN+wvvCD2KmHTkix1VB+sJDANBgkqhkiG9w0BAQsF
AAOCAQEABQFOfcwTQL2yaNMPonXvplJ8Bhkrqgza8Uq9Difw65EWLFn9ZjKoIcQ6
H9z1zC9dfKvhRfX3JVH3NhsInIDmkrNBGwJpGeQrwiasBOu/MmKFbPOu1pdpXMAR
dWUppI86xf3LX4H2f1jE9SD1URr7SPeIHkVLHpTwzgNRHfC91/gE4ncAXFc+cjou
2/M+TjZzQuwIujk4lHHLjZqYExusx0fFNfcnrCQVSepg1pdoENT4yYcfKcr4rrDH
vh5zeX8vVGLlG5jwuJiMOFBVgnKukR6RF9Xia2fQrTJi7X0gZj5qgT5kw+EWPkz7
V6zwsucv27pv93Gv5K8M2vKh7cqlvw==
-----END CERTIFICATE-----
"#;

const NON_CRITICAL_CLAIMS_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDWjCCAkKgAwIBAgIUSaB/actggodTq4A8CAjETDQ+4L8wDQYJKoZIhvcNAQEL
BQAwIDEeMBwGA1UEAwwVZGF0YS1ub2RlLW5vbmNyaXRpY2FsMB4XDTI2MDQxOTA4
MDAxMFoXDTI2MDQyMDA4MDAxMFowIDEeMBwGA1UEAwwVZGF0YS1ub2RlLW5vbmNy
aXRpY2FsMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAoTSBYrI8Z36h
ZrdlS49U/dwiPHy5PTtLEIcz4jpZti4ahensVHddWYUv1PVB87sgV95ZCA0VZsnz
7tA6fgoXVWhKfYExZqE1pyZL/h88RFQK+JEajj0hip6La9oJGYfdSn0LdjtTRRQT
pFYNIEWVqvhZ7wLmAsE1zZp4bJC02HGSXJv1ND50yes7ubXEYXnOzkbgKrmQ3ECV
85BpRa89okzMjHQvWflcqDcrpwwVd/nGz90OjQcDpDfFUaSdQfQVSPlDu9C8zqg4
jJpZ0PpeQ2L6yw/nH9/r6hqhe62FR4vzH7wwJEv7KT0NtwK8jBCWa+wFPwx4SJN9
6p5Vvei7KQIDAQABo4GLMIGIMCAGA1UdEQQZMBeCFW5vZGUtNDIuY2x1c3Rlci5s
b2NhbDBFBgorBgEEAYOyAwEBBDekZHJvbGVpZGF0YV9ub2RlZnNjb3Blc4FnY2x1
c3Rlcml0ZW5hbnRfaWT2ZmtleV9pZEQBAgMEMB0GA1UdDgQWBBTSZd7hQSp0/L+N
SQSIStejc/rE2DANBgkqhkiG9w0BAQsFAAOCAQEAUQV0Vv2LRS86fzhfrJN/vEOI
rHAtI4BFttl2+rIjs6zYxCW4ikuPxYrcix2ispiVt2aBGQSGoEudACRQNY3DOpnu
yCIXWDO5HiCgPWG87iKQKnhD94j9gfNq1qM32sXLqa+BDP9p4cjnrfDeel7O5wbv
S/nTlKQSPX21+0e5ZVVIo5Ht2ksW6+tCoYidV5l+uIdqTbIS0R+yUXK7XUeF8nWU
34Ty+du0sO9MLXCRGJBYuTepQGYZrNm4lSQ/iCzxDY0n5u4EENhbwTKWCI9O8me3
yA/fIMWhTLhpnMYbJPoDRjW/2PjlLJGNye3+mZiiuOBlTRrrqpnerlTqyk7kAg==
-----END CERTIFICATE-----
"#;

fn certs_from_pem(pem: &str) -> Vec<CertificateDer<'static>> {
	CertificateDer::pem_slice_iter(pem.as_bytes())
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
}

fn fixture_pki_root() -> &'static PathBuf {
	static ROOT: OnceLock<PathBuf> = OnceLock::new();
	ROOT.get_or_init(|| {
		let root =
			std::env::temp_dir().join(format!("wendelon-tls-identity-{}", uuid::Uuid::new_v4()));
		tls_pki::write_test_pki(&root);
		root
	})
}

fn certs_from_fixture(name: &str) -> Vec<CertificateDer<'static>> {
	let path = fixture_pki_root().join(name);
	CertificateDer::pem_file_iter(&path)
		.unwrap()
		.collect::<Result<Vec<_>, _>>()
		.unwrap()
}

fn trust_roots() -> ActorTrustRoots {
	let root = fixture_pki_root();
	ActorTrustRoots::load(root.join("ca.crt"), root.join("ops_ca.crt")).unwrap()
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

#[test]
fn transport_peer_parses_actor_claims_and_node_id() {
	let certs = certs_from_pem(DATA_NODE_CLAIMS_CERT_PEM);
	let peer = verified_transport_peer_from_chain(&certs, DEFAULT_CLUSTER_DOMAIN).unwrap();
	assert_eq!(
		peer.node_id,
		NodeId {
			id: 42,
			_reserved: 0,
		}
	);
	assert_eq!(peer.actor_identity.role, ActorRole::DataNode);
	assert_eq!(peer.actor_identity.node_id, Some(peer.node_id));
	assert_eq!(peer.actor_identity.key_id, vec![1, 2, 3, 4]);
	assert!(!peer.actor_identity.break_glass);
}

#[test]
fn transport_peer_rejects_missing_actor_claims_extension() {
	let certs = certs_from_pem(HAPPY_CERT_PEM);
	let err = verified_transport_peer_from_chain(&certs, DEFAULT_CLUSTER_DOMAIN).unwrap_err();
	assert!(err.to_string().contains("actor-claims extension"));
}

#[test]
fn transport_peer_accepts_non_critical_actor_claims_extension() {
	let certs = certs_from_pem(NON_CRITICAL_CLAIMS_CERT_PEM);
	let peer = verified_transport_peer_from_chain(&certs, DEFAULT_CLUSTER_DOMAIN).unwrap();
	assert_eq!(peer.actor_identity.role, ActorRole::DataNode);
	assert_eq!(peer.actor_identity.node_id.map(|node| node.id), Some(42));
}

#[test]
fn transport_peer_rejects_non_data_node_actor_role() {
	let certs = certs_from_pem(OPERATOR_CLAIMS_CERT_PEM);
	let err = verified_transport_peer_from_chain(&certs, DEFAULT_CLUSTER_DOMAIN).unwrap_err();
	assert!(err.to_string().contains("not allowed"));
}

#[test]
fn actor_identity_enforces_ca_domain_against_role_claims() {
	let trust_roots = trust_roots();
	let operator_on_data_ca = certs_from_fixture("operator_on_node_ca.crt");
	let err = verified_peer_actor_identity_from_chain_with_trust_roots(
		&operator_on_data_ca,
		DEFAULT_CLUSTER_DOMAIN,
		Some(&trust_roots),
	)
	.unwrap_err();
	assert!(err.to_string().contains("must chain to the ops CA"));

	let data_node_on_ops_ca = certs_from_fixture("data_node_on_ops_ca.crt");
	let err = verified_peer_actor_identity_from_chain_with_trust_roots(
		&data_node_on_ops_ca,
		DEFAULT_CLUSTER_DOMAIN,
		Some(&trust_roots),
	)
	.unwrap_err();
	assert!(err.to_string().contains("must chain to the data CA"));
}

#[test]
fn break_glass_identity_is_marked_when_signed_by_ops_ca() {
	let trust_roots = trust_roots();
	let certs = certs_from_fixture("break_glass.crt");
	let actor = verified_peer_actor_identity_from_chain_with_trust_roots(
		&certs,
		DEFAULT_CLUSTER_DOMAIN,
		Some(&trust_roots),
	)
	.unwrap();
	assert_eq!(actor.role, ActorRole::BreakGlassAdmin);
	assert_eq!(actor.trust_domain, ActorTrustDomain::Ops);
	assert_eq!(actor.cert_subject.as_deref(), Some("CN=break-glass"));
	assert!(actor.break_glass);
}

#[test]
fn authorization_matrix_enforces_ops_and_observability_boundaries() {
	let data_node = ActorIdentity::data_node(NodeId {
		id: 7,
		_reserved: 0,
	});
	let operator = ActorIdentity::new(
		ActorRole::Operator,
		None,
		None,
		vec!["cluster".into()],
		vec![9, 8, 7, 6],
	)
	.unwrap();
	let tenant_admin = ActorIdentity::new(
		ActorRole::TenantAdmin,
		None,
		None,
		vec!["cluster".into()],
		vec![5, 4, 3, 2],
	)
	.unwrap();
	let cluster_admin = ActorIdentity::new(
		ActorRole::ClusterAdmin,
		None,
		None,
		vec!["cluster".into()],
		vec![2, 3, 4, 5],
	)
	.unwrap();
	let backup_admin = ActorIdentity::new(
		ActorRole::BackupAdmin,
		None,
		None,
		vec!["cluster".into()],
		vec![7, 7, 7, 7],
	)
	.unwrap();
	let compliance_auditor = ActorIdentity::new(
		ActorRole::ComplianceAuditor,
		None,
		None,
		vec!["cluster".into()],
		vec![8, 8, 8, 8],
	)
	.unwrap();
	let observer = ActorIdentity::new(
		ActorRole::ReadOnlyObserver,
		None,
		None,
		vec!["cluster".into()],
		vec![6, 7, 8, 9],
	)
	.unwrap();

	assert!(authorize_actor(&data_node, ActorPermission::InterNodeTransport).is_ok());
	assert!(authorize_actor(&data_node, ActorPermission::ObservabilityReady).is_ok());
	assert!(authorize_actor(&data_node, ActorPermission::ObservabilityMetrics).is_err());
	assert!(authorize_actor(&data_node, ActorPermission::OpsControl).is_err());
	assert!(authorize_actor(&operator, ActorPermission::OpsControl).is_ok());
	assert!(authorize_actor(&tenant_admin, ActorPermission::TenantAdminMutation).is_ok());
	assert!(authorize_actor(&tenant_admin, ActorPermission::TenantKekAdmin).is_err());
	assert!(authorize_actor(&cluster_admin, ActorPermission::ClusterControl).is_ok());
	assert!(authorize_actor(&cluster_admin, ActorPermission::TenantAdminMutation).is_err());
	assert!(authorize_actor(&backup_admin, ActorPermission::TenantKekAdmin).is_ok());
	assert!(authorize_actor(&backup_admin, ActorPermission::ClusterControl).is_err());
	assert!(authorize_actor(&compliance_auditor, ActorPermission::AuditRead).is_ok());
	assert!(authorize_actor(&compliance_auditor, ActorPermission::OpsControl).is_err());
	assert!(authorize_actor(&operator, ActorPermission::ObservabilityMetrics).is_ok());
	assert!(authorize_actor(&operator, ActorPermission::InterNodeTransport).is_err());
	assert!(authorize_actor(&observer, ActorPermission::ObservabilityMetrics).is_ok());
	assert!(authorize_actor(&observer, ActorPermission::OpsControl).is_err());
}

#[test]
fn break_glass_scope_matrix_is_enforced() {
	let diagnostic = ActorIdentity::new(
		ActorRole::BreakGlassAdmin,
		None,
		None,
		vec![BreakGlassScope::Diagnostic.as_str().to_string()],
		vec![1, 2, 3, 4],
	)
	.unwrap();
	let key_ops = ActorIdentity::new(
		ActorRole::BreakGlassAdmin,
		None,
		None,
		vec![BreakGlassScope::KeyOps.as_str().to_string()],
		vec![5, 6, 7, 8],
	)
	.unwrap();
	let full = ActorIdentity::new(
		ActorRole::BreakGlassAdmin,
		None,
		None,
		vec![BreakGlassScope::Full.as_str().to_string()],
		vec![9, 9, 9, 9],
	)
	.unwrap();

	assert!(authorize_actor(&diagnostic, ActorPermission::ObservabilityMetrics).is_ok());
	assert!(authorize_actor(&diagnostic, ActorPermission::AuditRead).is_ok());
	assert!(authorize_actor(&diagnostic, ActorPermission::TenantAdminMutation).is_err());
	assert!(authorize_actor(&diagnostic, ActorPermission::ClusterControl).is_err());

	assert!(authorize_actor(&key_ops, ActorPermission::TenantKekAdmin).is_ok());
	assert!(authorize_actor(&key_ops, ActorPermission::ObservabilityMetrics).is_err());
	assert!(authorize_actor(&key_ops, ActorPermission::ClusterControl).is_err());

	assert!(authorize_actor(&full, ActorPermission::ObservabilityMetrics).is_ok());
	assert!(authorize_actor(&full, ActorPermission::TenantAdminMutation).is_ok());
	assert!(authorize_actor(&full, ActorPermission::TenantKekAdmin).is_ok());
	assert!(authorize_actor(&full, ActorPermission::ClusterControl).is_ok());
}

#[test]
fn revoked_actor_key_ids_are_rejected_by_identity_verifier() {
	let certs = certs_from_fixture("operator.crt");
	let roots = trust_roots();
	let mut revoked = BTreeMap::new();
	let revoked_key = RevokeActorKey {
		trust_domain: ActorTrustDomain::Ops,
		key_id: vec![5, 6, 7, 8],
		revoked_at_ms: 1,
	};
	revoked.insert(revoked_key.key(), revoked_key.revoked_at_ms);
	let snapshot = RevocationSnapshot {
		revoked_actor_keys: Arc::new(revoked),
	};
	let err = verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations(
		&certs,
		DEFAULT_CLUSTER_DOMAIN,
		Some(&roots),
		Some(&snapshot),
	)
	.unwrap_err();
	assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
	assert!(err.to_string().contains("revoked"));
}
