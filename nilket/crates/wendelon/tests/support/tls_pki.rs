#![allow(dead_code)]

use std::path::{Path, PathBuf};

pub const CLUSTER_DOMAIN: &str = "cluster.local";

pub struct TestPkiPaths {
	pub ca_cert: PathBuf,
	pub ops_ca_cert: PathBuf,
	pub server_cert: PathBuf,
	pub server_key: PathBuf,
	pub data_client_cert: PathBuf,
	pub data_client_key: PathBuf,
	pub operator_cert: PathBuf,
	pub operator_key: PathBuf,
	pub break_glass_cert: PathBuf,
	pub break_glass_key: PathBuf,
	pub operator_on_node_ca_cert: PathBuf,
	pub operator_on_node_ca_key: PathBuf,
	pub data_node_on_ops_ca_cert: PathBuf,
	pub data_node_on_ops_ca_key: PathBuf,
}

pub fn write_test_pki(dir: &Path) -> TestPkiPaths {
	std::fs::create_dir_all(dir).unwrap();
	let paths = TestPkiPaths {
		ca_cert: dir.join("ca.crt"),
		ops_ca_cert: dir.join("ops_ca.crt"),
		server_cert: dir.join("server.crt"),
		server_key: dir.join("server.key"),
		data_client_cert: dir.join("data_client.crt"),
		data_client_key: dir.join("data_client.key"),
		operator_cert: dir.join("operator.crt"),
		operator_key: dir.join("operator.key"),
		break_glass_cert: dir.join("break_glass.crt"),
		break_glass_key: dir.join("break_glass.key"),
		operator_on_node_ca_cert: dir.join("operator_on_node_ca.crt"),
		operator_on_node_ca_key: dir.join("operator_on_node_ca.key"),
		data_node_on_ops_ca_cert: dir.join("data_node_on_ops_ca.crt"),
		data_node_on_ops_ca_key: dir.join("data_node_on_ops_ca.key"),
	};
	std::fs::write(&paths.ca_cert, include_str!("pki/ca.crt")).unwrap();
	std::fs::write(&paths.ops_ca_cert, include_str!("pki/ops_ca.crt")).unwrap();
	std::fs::write(&paths.server_cert, include_str!("pki/server.crt")).unwrap();
	std::fs::write(&paths.server_key, include_str!("pki/server.key")).unwrap();
	std::fs::write(&paths.data_client_cert, include_str!("pki/data_client.crt")).unwrap();
	std::fs::write(&paths.data_client_key, include_str!("pki/data_client.key")).unwrap();
	std::fs::write(&paths.operator_cert, include_str!("pki/operator.crt")).unwrap();
	std::fs::write(&paths.operator_key, include_str!("pki/operator.key")).unwrap();
	std::fs::write(&paths.break_glass_cert, include_str!("pki/break_glass.crt")).unwrap();
	std::fs::write(&paths.break_glass_key, include_str!("pki/break_glass.key")).unwrap();
	std::fs::write(
		&paths.operator_on_node_ca_cert,
		include_str!("pki/operator_on_node_ca.crt"),
	)
	.unwrap();
	std::fs::write(
		&paths.operator_on_node_ca_key,
		include_str!("pki/operator_on_node_ca.key"),
	)
	.unwrap();
	std::fs::write(
		&paths.data_node_on_ops_ca_cert,
		include_str!("pki/data_node_on_ops_ca.crt"),
	)
	.unwrap();
	std::fs::write(
		&paths.data_node_on_ops_ca_key,
		include_str!("pki/data_node_on_ops_ca.key"),
	)
	.unwrap();
	paths
}

const CA_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDGzCCAgOgAwIBAgIUOBCzpmp3k+GcOkfDCDpm7FYz7DMwDQYJKoZIhvcNAQEL
BQAwFTETMBEGA1UEAwwKdzItdGVzdC1jYTAeFw0yNjA0MTkwODIxNTlaFw0yNjA0
MjAwODIxNTlaMBUxEzARBgNVBAMMCncyLXRlc3QtY2EwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQDFpfvVQgRBa3MitVgS9QVyOG7Tlt8qtXzG01palo7p
JyJ7zXD0P//N+NLR1Xylkc1mTaX1p8s++3P4MW/g2HZIBAvXCSKsjxtRqe7pIlqD
XFngFMEyVyM6iHr/uSvYko6CVVc1pXrprMLDZVVLVrTLUszI/8b24AmH3YUwTFXp
pqN3r/yt/Faz+xGwctXVmya42hmwyX/E2p1OdvPYYnTMaNSR1fMhoKcsWNrS+Vok
PZIuIzRz2IzkAGeJABaipKdUH+qrpBs0IAAjpf3m5PyOY+77KOf8Blk49n/qEIlY
LqTGJIzNkXKDWeH0YD7VYxEU9+Lbpg93NhU8Y3KIojJvAgMBAAGjYzBhMA8GA1Ud
EwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMB0GA1UdDgQWBBSQ7lxJ9K9Q2Da2
tDeUftHxp+0xUzAfBgNVHSMEGDAWgBSQ7lxJ9K9Q2Da2tDeUftHxp+0xUzANBgkq
hkiG9w0BAQsFAAOCAQEAUe1bCrrGNT29ekFw7Uc5yvUfoa0Y7ugfKChA059B7Z7e
KcAKNlVD5Ix5j3zCKKnRtNSjiGY6oTPOwcS+N96KZqR3iRJFa8yL3+grAIdNil77
m2fH0VwfgNXRcYdzAYx8/8VrRE6jDqj4dd97PIHIL4h9IqcKuZzB2PsCbK5s/dgm
5KkYwRhhGGWTckcQ92nMtI6J0qkoFGNFxla4Wkz3AXZtRWhjFLGCFESXERkwqvXD
iUNuQp1NbkqoyxUJz3lGolxjRI4pGXyXNh2QQ9MRYveGcCZxJw4tbMpofFDQ/FB4
ziUsxyBBJu7s1jlLyTrCWoq2xym/CRx54LeemFXVxQ==
-----END CERTIFICATE-----
"#;

const SERVER_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDYzCCAkugAwIBAgIUcQJ40MlGe9OjM1Q7CYzB/iKK6/owDQYJKoZIhvcNAQEL
BQAwFTETMBEGA1UEAwwKdzItdGVzdC1jYTAeFw0yNjA0MTkwODIyMjJaFw0yNjA0
MjAwODIyMjJaMBExDzANBgNVBAMMBm5vZGUtMTCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBALOOLzvfDSfmH3WJiUTlYJoYC3a99YMnoeimxTR9515rWCj7
CR6dXvmSUkZDjXEp4/8aM/feUtqE8oH/VdNyCAgen1FSONuBvqexkec0rbEbZlUt
ZDS2Blm7Yp3mGQfgC7/iJJ/uJnTjxlWIIx7Wva4wx+LAJG4++iVvq5BsIB4T9FYe
Bqg9sKENODQqzJKePFRmCT1IbqU2a6LQZKghc0pQ0e9SLG31jcP3Wuohob+sWzqx
7mY+8L3F7lfvodBOK39S6e++ceDzMgJMc+rtBMzhRBVapoyKZKNsvYhxq5cCgemp
j4sd3fIubaHwcJVTDWcip0zj3l2LyACh7Hs7IVsCAwEAAaOBrjCBqzAfBgNVHREE
GDAWghRub2RlLTEuY2x1c3Rlci5sb2NhbDBIBgorBgEEAYOyAwEBAQH/BDekZHJv
bGVpZGF0YV9ub2RlZnNjb3Blc4FnY2x1c3Rlcml0ZW5hbnRfaWT2ZmtleV9pZEQB
AgMEMB0GA1UdDgQWBBRzE9NelWz9zQHR9CfipmiaeQlPaTAfBgNVHSMEGDAWgBSQ
7lxJ9K9Q2Da2tDeUftHxp+0xUzANBgkqhkiG9w0BAQsFAAOCAQEATXcDq7xn+Ex3
j0xI/wXTvtf66loSxVFcH9XUdqx7bSEhbrgFinMmFe3DrYK8U3vTkGLE7k2p1H2Z
Tq0wLW3+B+ieKIhae8lA1Ntg6lvtIEnRD6hzKY+nYCeV0+2PnRRygsEaCyxftmRm
aihAcHdaojLDU3IPP7XSr01Z33chXA/fUtjPzBEpFWUx1x3EfSTPPCd3xtw65fuv
HGwJ2kOTrqpUy5pgJlkIds47WbUAPXr4SkXxKw+ZrEkp5qngCIYsWkl4CQaHUN4O
PpkG/oGmhz6o3f/Ii7sBNvcUVfEiJcyg3L1ZhTJHlZbR943CrqUKry6Iiq9JFh8L
6z5MoqI1tQ==
-----END CERTIFICATE-----
"#;

const SERVER_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCzji873w0n5h91
iYlE5WCaGAt2vfWDJ6HopsU0fedea1go+wkenV75klJGQ41xKeP/GjP33lLahPKB
/1XTcggIHp9RUjjbgb6nsZHnNK2xG2ZVLWQ0tgZZu2Kd5hkH4Au/4iSf7iZ048ZV
iCMe1r2uMMfiwCRuPvolb6uQbCAeE/RWHgaoPbChDTg0KsySnjxUZgk9SG6lNmui
0GSoIXNKUNHvUixt9Y3D91rqIaG/rFs6se5mPvC9xe5X76HQTit/UunvvnHg8zIC
THPq7QTM4UQVWqaMimSjbL2IcauXAoHpqY+LHd3yLm2h8HCVUw1nIqdM495di8gA
oex7OyFbAgMBAAECggEAKCs/cuf4E/x9/jgKLxEXtvF5IxPlKHXF2Xvxs0gVakCe
hPCAPkqblD7BTDnRm0bxXEE67hFdPMiwThfW1gp1NIfRQYqg8K6Kfg6SIwTcdLo5
/wU8HTS5n/p1Wfi/kcz0I/XOFr9a+Bx64uilXsFQxvbfyOEOZWGOabCdU5504vVY
mluwrC16HtqLdlqgdp2hU8GO4345kRXWa430/5Cn+szCqP//WsQHmx6GYiLuLNFb
hE7alk6yU8bd44gwCMX5iVYhTZtwWYJNF2DM4sQ5uSHL1RFp4TWjvXJxEWf4LDnD
7gRDErBlMs8IllodcxwN+/Vtn8eUOOIQEaImXibtbQKBgQDo8qmv/p3YIP4XN1Gc
r5ztWeU42jBdOUw6siwVO1E0vpdiEP9xORAEfoOflMeHz9/dcbXdS9OPpq1TvHtn
rX8IOKkABLQbWiR2jdh//5pVe4K47YNcYu32j95plKWAumRtmjfgCMOaV0K+DKE6
mLsmOtOKAoYw0zH8UtQHqrjqJwKBgQDFUun/NEttQDhLsIjsZZwHW1MiYbH3MK5a
b+61mI7785OOhCLLpCq8NdTJeUfU/SNg7+gjIyUUhEz3NjlrFnSduy9NYzfXYPM7
aZgir8D+MvPTDWssic937nLlnLPytGyO3upPrVCwWMjB77nhueb0ZjpKhM/jZguF
KRTqSHwTrQKBgFlomhyEu4yyzROYFejvtmmbMVGq3RlK3qlArY7Qn5CA/pWTNJf+
cNlovZhIgHTuIlxaQpVXfzIJpUkYdL0MaE99f9AT/hTpeklOlCQ8tL3OlBPqhZq2
FeOmwJUBiqxn3hf7Myr38iuRBnrvWgqliF9aAMtdwKeIyAyytdLjgenNAoGAGrKM
LFZD+kUdGMiKzgk1Yh1uExHKxVoVzPCmCHXaqqYbCcZA+vJ8Qbi60IOTP3L8bWoo
K1De/Hal/zCb9iw0nP6teLf63y4j2p7/0XmcEAiOyrhfDsiahGyDqSNZiXX11iK8
6wjVCPcKOosXyD2hzOFj858ijW7KEjsrtEMYXoUCgYAfnhaSYH5mORDC6GF9zeEW
MZtKKb09cYEUqFnDZKZP7MuHzrZ7VQni20rsGkk5X00SebUf/GoXTnRs/f79J2+K
7Yz+cvjuPMM0KwRlW9tYdSvJQ7mBPiALPFrFIEI6Ma/h1Tb8B8zxwX2srYBdx8QY
OeTIP0Z3xHQar/jWGUeCEA==
-----END PRIVATE KEY-----
"#;

const DATA_CLIENT_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDYzCCAkugAwIBAgIUcQJ40MlGe9OjM1Q7CYzB/iKK6/swDQYJKoZIhvcNAQEL
BQAwFTETMBEGA1UEAwwKdzItdGVzdC1jYTAeFw0yNjA0MTkwODIyMjJaFw0yNjA0
MjAwODIyMjJaMBExDzANBgNVBAMMBm5vZGUtMjCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBAK+VQnoNUQkEO41KtiXGcL/n6BFod2sUN7sTtDaElGc3PmXh
KyTyWQOtG0Wdnc+NaHsEjyA/IiorJMo8MmKFxb/1HiKk0IWI4l4D2rI1War2O3P2
Et/fZg/DPBR94J8i4HyLardaof9Id0IaBnf8Q8eEbLUE0rqCdYJhHSCFzKstPwiY
+quLG6bpDuN0uSF3ahl4cDFNCYoA0x6ZR/ZxdTeV6nkUYvUfHlxvcot8SgHMtAjB
x4Y8afohjKIZAaZfkXY1BaRkSEmi4h3aYFCpolvUD72BcfrVpS8XU7Jw9Qz9l385
WyxNkVY3Oh9tnikm0S1Q6cmCdIEXGeh8w3Ckv2UCAwEAAaOBrjCBqzAfBgNVHREE
GDAWghRub2RlLTIuY2x1c3Rlci5sb2NhbDBIBgorBgEEAYOyAwEBAQH/BDekZHJv
bGVpZGF0YV9ub2RlZnNjb3Blc4FnY2x1c3Rlcml0ZW5hbnRfaWT2ZmtleV9pZEQF
BgcIMB0GA1UdDgQWBBQo5VA49jtbGG/HBZzjB6cds3ufNDAfBgNVHSMEGDAWgBSQ
7lxJ9K9Q2Da2tDeUftHxp+0xUzANBgkqhkiG9w0BAQsFAAOCAQEAo7MQE5+tA1Mf
ujQ5wieJea9QNnQaEHM5nWTGjuaSr6+eS7KZhEQpv2FSk3aYhAqwJIb43ZfWooeO
OWzv0MdcUZ3sH+SzHVFZgTw4675BCYColxfBBJGC+voyq4GboyaOksgN2/YHCroo
VrPPuH8FjqGUrHPRmXW6XFuEhouyf4T2O1tsZAYgMxdwMem57iJlCrDp+UiKnMEn
ncLJFqs8whHAWTtsIZzQ+e+TzrCngHPhYYwAJI2gPlh4HaoBjtqWIoHhMRdZ4Q3n
iHVsYwJ3SvMZnED5C9Z658Kx+bAva9OdHji9B+Du5l0Tv0Q6wqYaksPRtBWwibrp
tBXrVU3qxA==
-----END CERTIFICATE-----
"#;

const DATA_CLIENT_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCvlUJ6DVEJBDuN
SrYlxnC/5+gRaHdrFDe7E7Q2hJRnNz5l4Ssk8lkDrRtFnZ3PjWh7BI8gPyIqKyTK
PDJihcW/9R4ipNCFiOJeA9qyNVmq9jtz9hLf32YPwzwUfeCfIuB8i2q3WqH/SHdC
GgZ3/EPHhGy1BNK6gnWCYR0ghcyrLT8ImPqrixum6Q7jdLkhd2oZeHAxTQmKANMe
mUf2cXU3lep5FGL1Hx5cb3KLfEoBzLQIwceGPGn6IYyiGQGmX5F2NQWkZEhJouId
2mBQqaJb1A+9gXH61aUvF1OycPUM/Zd/OVssTZFWNzofbZ4pJtEtUOnJgnSBFxno
fMNwpL9lAgMBAAECggEAAxKuriorf+fmDuv+0hxt/r3VTvCwHlylOQ34OngJjODn
o6rUMXpYWnseHhLLH0XPTU/gHWVdeLV2mF+DVjwXIOvuUhbmW1/w7kpM+o96eUaD
9dyZqfdj9uSo2Yb2dnKK0UMzY/AfI7Ol+q8v68ureAN2T5i9A2yjF9wRQW92DIzf
ua2gi7QEk6kmSnK6YyY6cTydy6Txczzs1BWNr60Ic7YHasD95auWE8ZxWs0AxZXh
oCkBWaNYmBVDOK3NVqnfjBWs02oIQj1Yd0QFkpRAkjctkG+hMxqgtYXzOxoPRiBd
4eeP7CbwzW3Nr+3+mngblQKFepXrro/yt/udxnAegQKBgQDnEFXh5vKH9cUDS9be
3mshzkYYvjGrMF/swJur7/DdSihB1wuHwQxzXTXaJmNIGKZ2ilaxx+BoJNkZPi5e
IF/PCadkvA2aggGrorFkTm/FBO66hVvfFLozFqgFshYoakYEgnPp6pHayZPYnU8E
b3u1lkJzSIqomwvI3yzi9hDfBQKBgQDCiCQ/FUPs0R5AzS2f0DDA76zdwJzMV8Jd
GkdQT/6q9K+x1Q3YJ3diJ/PuRODb1kFsIac7it6+x7NKjzXZp6dULVcwn2MG/tih
QMxlcaMtXE7zjD8ZUblIjFjROd+DnFMi4o8FvhTGwoPiknvGe6+PZmC2Ftz0MDnm
lvanjZKM4QKBgGuG3JCNytuecZpBFrMMSkqnFVO6Xp7NmjYww8DVnf883VMWYdVA
7yxYehEUwCry3bRYWFPPMg0E6MqodydyZWORmIZ2RNZcHa7jvS1ymuzSiM3f9Yrw
uF3B81sDLvCL1B2RH6QBdvKbz+OEARuNTcKJNE81uOERlgBaaycLX3qJAoGAUzHe
LoxtrE4S3ivho0NqVlkqWPwTEuFFYoPmk9ZiV8KlkSxI63SVfgkQLYqPj/UlrZUG
7FQLxxOo/e4ZXahxy3JIo6bGtysugmgwAgKDgtbDfS0xnyZwUzKSmYhiquqkM3sp
J4+PWyG9PZzgCpbDElT6b2V/wX8OhCtz+Vx2nIECgYAd3Z/20QM3qE6kfvsYH90d
dFXlOeGi4oW7JD5treUzBwiQ4AMlHQcZJdbpxlRVc725j9XiSP+oxA11+8nsyyiM
dw+sF4lkBQPr+KR8Kjjn/ZEfwTBkHFNyy727ExTB9v+gQ3WF64laZK8tUQOGOSX+
ME/MR3+4L6Cdii6I0duxDg==
-----END PRIVATE KEY-----
"#;

const OPERATOR_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDQzCCAiugAwIBAgIUcQJ40MlGe9OjM1Q7CYzB/iKK6/wwDQYJKoZIhvcNAQEL
BQAwFTETMBEGA1UEAwwKdzItdGVzdC1jYTAeFw0yNjA0MTkwODIyMjJaFw0yNjA0
MjAwODIyMjJaMBMxETAPBgNVBAMMCG9wZXJhdG9yMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAnJegLg1d5kc5/hAxf+nPl4YrFvervgI1nUk0YLCeQ7FE
Yb26LdkRanYayufuQ04aNLTlZy3gfyxiYieani8JWa4/2yrEXSORbxv1AEeKelb4
2A9fGd7COB+lhJ9OuzJZSNgc1w5Caead9BiP/1/OjgZUUYZjcYmE1j0OcDstkZNU
4Fy3EO+RLR4Jv5Ps+zOEoUSc1P/mv931RmT05AwlwcBIbFHhrUzGWp434Sv3/im3
cQrFy3cIVkelkwlUYZVTFJVC3coYIHfjE3O6xXH9T4yEM0Os6lcgOuSjb6n2MGAF
fkU8osxif+PBT6URFac/wCU7KQXxnF4HesDmoBc3qwIDAQABo4GMMIGJMEcGCisG
AQQBg7IDAQEBAf8ENqRkcm9sZWhvcGVyYXRvcmZzY29wZXOBZ2NsdXN0ZXJpdGVu
YW50X2lk9mZrZXlfaWRECQgHBjAdBgNVHQ4EFgQUmfxpoXTzrjcIhZLmI+UNjivN
xPgwHwYDVR0jBBgwFoAUkO5cSfSvUNg2trQ3lH7R8aftMVMwDQYJKoZIhvcNAQEL
BQADggEBAKY/wdeokQo2po3Bv9YLBtXALGk1RB4jisf1k/LAFaLbHSdpumIFkicW
72A0ErvZjXozdiM4C3a4ZF5IZzViA+KyoY2O2+cSTatUrimAEjOAgoKr0vmVLWvM
NoQvHUL01L52+BUcyUISGf5+4C20zCZC/ZrjUpzG67o/LRZex14iYS8TURImExs6
/2dZEQnPfBTGAh26LY/sthBLBxoVuqw8ovrhmrpcZSjSHaYyTJpSSr9wxMU5qPJP
AA+V0fHqw/tE2yG3GxlI0ZygOvKXVuX8QWok4dpKU1flflx+OwNRFoLnwPIjD0eU
0x/5COcMzCsn6YhypDdcOT4RE8flDBQ=
-----END CERTIFICATE-----
"#;

const OPERATOR_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCcl6AuDV3mRzn+
EDF/6c+XhisW96u+AjWdSTRgsJ5DsURhvbot2RFqdhrK5+5DTho0tOVnLeB/LGJi
J5qeLwlZrj/bKsRdI5FvG/UAR4p6VvjYD18Z3sI4H6WEn067MllI2BzXDkJp5p30
GI//X86OBlRRhmNxiYTWPQ5wOy2Rk1TgXLcQ75EtHgm/k+z7M4ShRJzU/+a/3fVG
ZPTkDCXBwEhsUeGtTMZanjfhK/f+KbdxCsXLdwhWR6WTCVRhlVMUlULdyhggd+MT
c7rFcf1PjIQzQ6zqVyA65KNvqfYwYAV+RTyizGJ/48FPpREVpz/AJTspBfGcXgd6
wOagFzerAgMBAAECggEAQNaHfIoMV00mtgv+cvhc/GCRdvEWK5a+nGGzTlXTMjQ0
HERy0okOrQY6Ecn23ed48ln0osRcMEwwcHP1kYnoVTnRNu5+YBJaJadtyq/UptNc
NhonRo713Ec+Qau07c3rKj5Rsro0xaS3wlppRVRmMmhsgVUj198ZIZwmXlIijCPO
wAYtRK+pNbGR1BrNyIb/bsCtKCnU9/R0629xuH+AEwT85KXYTSlMuFYjSNNuq2RP
gwbIElaffiiVDNzCf83qvUfK3DtsQlKD0w3cJCA1f+PIHvFJEQCxoVdSE8bR8bfL
wo/3Dz6A5TH3SyxGaT8V3FSBQhGl41gNeniH7q7QAQKBgQDXfFev+qeMhQATXpCW
zzJ67PuWpVlpyIrSMO2GSeOwi30fP2aaYd9Y6alNYjndTGzoULtKB3CEc7nzJhJa
yTSQkAx/4+4xwxEAmnm62B67KPr1/DxjtTyw42rTKpwZf6aucCZIaUWMFVU6+Y18
Xispv/EworOR4ZM5aOdicDLINwKBgQC6CKcD9zdIh91JpWwS0diCjj8ZubKXNqHO
28PWEw1brcMCvrRdJy+ZMwr+u2kgvjzbbzHLOhNT/aWqrRk+wny8X9TCots3SrJM
Yd3H6IeLghqJW+u9HEGnxnxY63X1g/PuDUFt5H47g292qkjsqiZbx6cVLf4SjOzC
bFvopbcqLQKBgB4gwXXWWtS3x8Hth9O+FnW/apGufqjepCItOxh6fj4x0oWq7KKL
KpoY2VX7dNGvHM8Ccp0wGxg5wfbwDMLqvNG1gjnjWWtVtava7UglbW6HlUcNRDhA
n8KvmUD7BSY/hcN/5KpdJZ8nYGpFd/W6iNhmgMdnCCpOWrI9K5TE0STTAoGAPBZv
W6iOAq9fmloD/lApToIDf5urQ4wx107EgnDE78fAJTLTrwLpLOJ/uqdt3VWU8ouH
2cx4UbirueUPMmY/Q9jwL63KgKFv1ZwGtns0zKrJR3SZQAvQaRCDOXVUNGjacRl2
+LfEzhQ1/G4j/801do+WOBES74r1Lhcx2IMuFkECgYEAiXbcgQquDPbcjiGoNqP6
xNEGCo3D9VPnPFR8qRi/ufsRx9xFCqNcBi9p+MDwqHQhBA5XGAlcgsYPUxyzPOfV
g75Czx/zyG42ZYrSyEEPaoFTcEPjh+lhkFjJbPB6VtFuT1sUe6x9zNClWTzAUMQU
AXz7HbY7vEE5nLQrYWBMfFI=
-----END PRIVATE KEY-----
"#;
