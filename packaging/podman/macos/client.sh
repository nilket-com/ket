#!/usr/bin/env bash
# Issue and check a Mac-owned Polariton client identity for local Nilket.
set -euo pipefail
umask 077

container=nilket-dev
entrypoint=/usr/local/bin/nilket-container
state="$HOME/Library/Application Support/polariton/nilket-podman"
key="$state/client-1.key"
cert="$state/client-1.crt"
ca="$state/ca.crt"
scratch=
die() { printf 'nilket client: %s\n' "$*" >&2; exit 1; }
cleanup() { [[ -z $scratch ]] || rm -rf "$scratch"; }
trap cleanup EXIT

[[ $(uname -s) == Darwin ]] || die 'client.sh supports macOS'
[[ ! -L $state ]] || die "state directory is a symlink: $state"
mkdir -p -m 700 "$state"
[[ $(stat -f %u "$state") == "$(id -u)" ]] || die "state directory is not owned by this user: $state"
chmod 700 "$state"
scratch=$(mktemp -d "$state/.tmp.XXXXXX")
[[ ! -L $key && ! -L $cert && ! -L $ca ]] || die 'identity paths must not be symlinks'

field() {
	local name=$1 lines
	lines=$(grep "^$name=" <<< "$status" || true)
	[[ $(wc -l <<< "$lines") -eq 1 && -n $lines ]] || die "container status lacks one $name field"
	printf '%s' "${lines#*=}"
}
verify_identity() {
	local candidate=$1 current_ca=$2
	[[ $(stat -f %u "$key") == "$(id -u)" && $(stat -f %Lp "$key") == 600 ]] ||
		die "$key must be owned by this user with mode 0600"
	openssl verify -CAfile "$current_ca" "$candidate" >/dev/null ||
		die 'client certificate is not trusted by the current container CA'
	cmp -s \
		<(openssl pkey -in "$key" -pubout -outform DER) \
		<(openssl x509 -in "$candidate" -pubkey -noout | openssl pkey -pubin -outform DER) ||
		die 'client certificate does not match the local private key'
	openssl x509 -in "$candidate" -noout -text |
		grep -Eq 'DNS:client-1\.cluster\.local([,[:space:]]|$)' ||
		die 'client certificate does not name tenant 1'
	openssl x509 -in "$candidate" -noout -checkend 0 >/dev/null ||
		die 'client certificate has expired'
}

status=$(podman exec "$container" "$entrypoint" status) || die 'cannot read container status'
server_name=$(field server_name)
[[ $server_name =~ ^toron-[[:xdigit:]-]{36}\.cluster\.local$ ]] ||
	die "invalid server name from container: $server_name"
[[ $(field tenant) == 1 && $(field sql_port) == 7742 && $(field sql_ready) == yes ]] ||
	die 'container is not accepting tenant-1 SQL on UDP 7742'
podman exec "$container" "$entrypoint" ca > "$scratch/ca.crt" ||
	die 'cannot retrieve the container CA certificate'
openssl x509 -in "$scratch/ca.crt" -noout >/dev/null || die 'container CA certificate is invalid'
if [[ -e $ca ]]; then
	saved=$(openssl x509 -in "$ca" -noout -fingerprint -sha256) || die 'saved CA is invalid'
	current=$(openssl x509 -in "$scratch/ca.crt" -noout -fingerprint -sha256)
	[[ $saved == "$current" ]] ||
		die 'container CA differs from the saved CA; use a new client state directory'
fi

if [[ ! -e $key ]]; then
	[[ ! -e $cert ]] || die 'certificate exists without its private key'
	openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 \
		-pkeyopt ec_param_enc:named_curve \
		-out "$scratch/client.key" >/dev/null ||
		die 'cannot generate a private key'
	ln "$scratch/client.key" "$key" || die 'refusing to overwrite an existing private key'
fi
if [[ ! -e $cert ]]; then
	# macOS LibreSSL otherwise emits explicit EC parameters in the CSR.
	# rcgen's ring verifier accepts the named P-256 encoding.
	public_params=$(openssl pkey -in "$key" -pubout -outform DER |
		openssl asn1parse -inform DER) || die 'cannot inspect the client public key'
	[[ $public_params == *':prime256v1'* ]] ||
		die 'client key must use named P-256 parameters; move it aside and rerun'
	openssl req -new -key "$key" -subj /CN=macos-desktop \
		-out "$scratch/client.csr" >/dev/null ||
		die 'cannot create a CSR'
	podman exec -i "$container" "$entrypoint" sign-client --tenant 1 \
		< "$scratch/client.csr" > "$scratch/client.crt" ||
		die 'container could not sign the CSR'
	verify_identity "$scratch/client.crt" "$scratch/ca.crt"
	ln "$scratch/client.crt" "$cert" || die 'refusing to overwrite an existing certificate'
fi
verify_identity "$cert" "$scratch/ca.crt"
if [[ ! -e $ca ]]; then
	ln "$scratch/ca.crt" "$ca" || die 'refusing to overwrite an existing CA certificate'
fi

printf 'endpoint=127.0.0.1:7742\nserver_name=%s\nclient_cert=%s\nclient_key=%s\nca_cert=%s\n' \
	"$server_name" "$cert" "$key" "$ca"
