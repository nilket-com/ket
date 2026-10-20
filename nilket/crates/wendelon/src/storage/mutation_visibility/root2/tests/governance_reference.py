#!/usr/bin/env python3
"""Independent VISADG01 and runtime lease LE vectors.
Run: python3 -I governance_reference.py > governance_vectors.json
Inputs: the M2 profile2 manifest (admission42/governance92, inlined) and genesis; the `p3_` half
uses the M3_PRIMARY manifest (governance seed 12, inlined) on the same generation and epoch.
Constructs Bootstrap payload/digests and full context independently of Rust.
Lease fields, LE offsets, zero reserved words and RFC8032 signatures are explicit.
"""
import hashlib
import json
import pathlib
import struct
import sys

import cryptography
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

HERE = pathlib.Path(__file__).resolve().parent
SV = json.loads((HERE / "statement_vectors.json").read_text())
SEED = bytes([0x92]) * 32
DOMAIN = b"VISROOT2-GOVERNANCE-COMMAND\x00"
STATEMENT_DOMAIN = b"VISROOT2-ADMIN-STATEMENT\x00"
L = 2**252 + 27742317777372353535851937790883648493


def sha(*parts):
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.digest()


key = Ed25519PrivateKey.from_private_bytes(SEED)
public = key.public_key().public_bytes_raw()
# The M2 profile-2 manifest "b" (admission 0x42, governance 0x92), inlined.
manifest = bytes.fromhex(
    "56495341444d320001000200424242424242424242424242424242424d4d4d4d"
    "4d4d4d4d4d4d4d4d4d4d4d4d4201000000000000717171717171717171717171"
    "717171712152f8d19b791d24453242e15f2eab6cb7cffa7b6a5ed30097960e06"
    "9881db1203000000050000000000000000000000000000000000000000000000"
    "00000000000000000091919191919191919191919191919191c6d0c796bb4c3c"
    "923615f8dc85e6fc35149eb6b1317de523d3a5085584f5c109ff010000"
)
assert len(manifest) == 189 and manifest[153:185] == public
g, c = manifest[12:28], manifest[28:44]
epoch = struct.unpack("<Q", manifest[44:52])[0]
pin = sha(b"VISROOT2-ADMIN-MANIFEST\x00", manifest)
bootstrap = g + c + b"\x00" + struct.pack("<Q", epoch) + pin + struct.pack("<Q", 189)
digest = sha(b"VISROOT2-COMMAND\x00", bootstrap)
fp = sha(b"TEST-S1-ENTRY\x00", bootstrap)
genesis = bytes.fromhex(SV["genesis"])
context = {
    "generation": g, "cluster": c, "term": 1, "node": 1, "index": 2,
    "fingerprint": fp, "command": digest, "genesis": genesis, "epoch": epoch, "pin": pin,
}


def body(ctx, command_digest):
    return (b"VISADG01" + struct.pack("<HH", 1, 0) + ctx["generation"] + ctx["cluster"]
            + struct.pack("<QIQ", ctx["term"], ctx["node"], ctx["index"])
            + ctx["fingerprint"] + ctx["command"] + ctx["genesis"]
            + struct.pack("<Q", ctx["epoch"]) + ctx["pin"] + command_digest)


def signed(ctx, command_digest, domain=DOMAIN):
    b = body(ctx, command_digest)
    assert len(b) == 232
    return b + key.sign(domain + b)


D1 = sha(b"sample-admin-command-1")
D2 = sha(b"sample-admin-command-2")
valid = signed(context, D1)
assert len(valid) == 296

variants = {}
for field, value in [
    ("generation", bytes([0x5B]) * 16), ("cluster", bytes([0x5C]) * 16),
    ("term", 2), ("node", 3), ("index", 4),
    ("fingerprint", bytes([0x61]) * 32), ("command", bytes([0x62]) * 32),
    ("genesis", bytes([0x63]) * 32), ("epoch", epoch + 1), ("pin", bytes([0x64]) * 32),
]:
    ctx = dict(context)
    ctx[field] = value
    variants[field] = signed(ctx, D1).hex()

r = int.from_bytes(valid[232:264], "little")
s = int.from_bytes(valid[264:296], "little")
noncanonical = valid[:264] + (s + L).to_bytes(32, "little") if s + L < 2**256 else None

# Independent explicit runtime LE prefix; reserved words are deliberately zero.
lease_fields = [0x0102030405060708, 0x11121314, 0x2122232425262728, 0x3132333435363738,
                0x414243444546, 0x51525354, 0x61626364, 0x717273747576,
                0x81828384, 0x91929394, 0xa1a2a3a4a5a6a7a8, 0xb1b2b3b4]
lease_payload = struct.pack("<QI I QQ QII QII QII", lease_fields[0],lease_fields[1],0,
                            *lease_fields[2:11],lease_fields[11],0)
assert len(lease_payload) == 80
issuer = Ed25519PrivateKey.from_private_bytes(bytes([0xa7])*32)
lease_signature = issuer.sign(lease_payload)
lease_bad_s = lease_signature[:32] + (int.from_bytes(lease_signature[32:],"little")+L).to_bytes(32,"little")

# ---- The profile-3 (M3_PRIMARY) governance half: the same construction, governance seed 12 ----
P3_SEED = bytes([12]) * 32
p3_key = Ed25519PrivateKey.from_private_bytes(P3_SEED)
p3_public = p3_key.public_key().public_bytes_raw()
# M3_PRIMARY (admission 0x42 base, admin seed 11, governance 0xd2/12, recovery 0xd3/13), inlined.
p3_manifest = bytes.fromhex(
    "56495341444d320001000300424242424242424242424242424242424d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d"
    "42010000000000007171717171717171717171717171717166be7e332c7a453332bd9d0a7f7db055f5c5ef1a"
    "06ada66d98b39fb6810c473a0300000005000000000000000000000000000000000000000000000000000000"
    "0000000000d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d20b513ad9b4924015ca0902ed079044d3ac5dbec2306f06"
    "948c10da8eb6e39f2dff010000d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d391a28a0b74381593a4d94695792089"
    "26afc8ad82c8839b7644359b9eba9a4b3a1f000000"
)
assert len(p3_manifest) == 241 and p3_manifest[153:185] == p3_public
assert p3_manifest[12:44] == manifest[12:44] and p3_manifest[44:52] == manifest[44:52]
p3_pin = sha(b"VISROOT2-ADMIN-MANIFEST\x00", p3_manifest)
p3_bootstrap = g + c + b"\x00" + struct.pack("<Q", epoch) + p3_pin + struct.pack("<Q", 241)
p3_digest = sha(b"VISROOT2-COMMAND\x00", p3_bootstrap)
p3_fp = sha(b"TEST-S1-ENTRY\x00", p3_bootstrap)
p3_context = dict(context, fingerprint=p3_fp, command=p3_digest, pin=p3_pin)


def p3_signed(ctx, command_digest, domain=DOMAIN):
    b = body(ctx, command_digest)
    return b + p3_key.sign(domain + b)


p3_valid = p3_signed(p3_context, D1)
p3_variants = {}
for field, value in [
    ("generation", bytes([0x5B]) * 16), ("cluster", bytes([0x5C]) * 16),
    ("term", 2), ("node", 3), ("index", 4),
    ("fingerprint", bytes([0x61]) * 32), ("command", bytes([0x62]) * 32),
    ("genesis", bytes([0x63]) * 32), ("epoch", epoch + 1), ("pin", bytes([0x64]) * 32),
]:
    p3_variants[field] = p3_signed(dict(p3_context, **{field: value}), D1).hex()
p3_s = int.from_bytes(p3_valid[264:296], "little")
p3_noncanonical = p3_valid[:264] + (p3_s + L).to_bytes(32, "little") if p3_s + L < 2**256 else None

json.dump({
    "generator": "governance_reference.py",
    "provenance": {"python": sys.version.split()[0], "cryptography": cryptography.__version__,
                   "primitive": "cryptography Ed25519PrivateKey (RFC 8032 pure Ed25519)",
                   "inputs": ["statement_vectors.json", "../provisioning/tests/profile_two_inventory_vectors.json"], "test_seed": SEED.hex()},
    "governance_public": public.hex(),
    "manifest": manifest.hex(),
    "pin": pin.hex(),
    "bootstrap_payload": bootstrap.hex(),
    "bootstrap_command": digest.hex(),
    "bootstrap_fingerprint": fp.hex(),
    "context": {k: (v.hex() if isinstance(v, bytes) else v) for k, v in context.items()},
    "lease_fields": lease_fields, "lease_payload": lease_payload.hex(),
    "issuer_public": issuer.public_key().public_bytes_raw().hex(),
    "lease_signature": lease_signature.hex(), "lease_noncanonical_s": lease_bad_s.hex(),
    "digest_1": D1.hex(),
    "digest_2": D2.hex(),
    "valid": valid.hex(),
    "preimage": (DOMAIN + valid[:232]).hex(),
    "context_variants": variants,
    "statement_domain_signature": signed(context, D1, STATEMENT_DOMAIN).hex(),
    "noncanonical_s": noncanonical.hex() if noncanonical else None,
    "p3_governance_public": p3_public.hex(),
    "p3_manifest": p3_manifest.hex(),
    "p3_pin": p3_pin.hex(),
    "p3_bootstrap_payload": p3_bootstrap.hex(),
    "p3_bootstrap_command": p3_digest.hex(),
    "p3_bootstrap_fingerprint": p3_fp.hex(),
    "p3_context": {k: (v.hex() if isinstance(v, bytes) else v) for k, v in p3_context.items()},
    "p3_valid": p3_valid.hex(),
    "p3_preimage": (DOMAIN + p3_valid[:232]).hex(),
    "p3_context_variants": p3_variants,
    "p3_statement_domain_signature": p3_signed(p3_context, D1, STATEMENT_DOMAIN).hex(),
    "p3_noncanonical_s": p3_noncanonical.hex() if p3_noncanonical else None,
}, sys.stdout, indent=2)
sys.stdout.write("\n")
