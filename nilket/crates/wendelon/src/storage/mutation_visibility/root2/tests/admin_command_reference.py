#!/usr/bin/env python3
"""Independent reference for the VISADC01 administrator command envelope vectors.

Run from this directory: python3 -I admin_command_reference.py > admin_command_vectors.json

Derives everything from checked-in inputs (statement_vectors.json: manifest_prime, genesis) and a fixed
TEST administrator seed, without the Rust code under test:
  manifest  = manifest_prime with admin_key (bytes 68..100) := Ed25519 public key of the seed
  pin       = sha256("VISROOT2-ADMIN-MANIFEST\\0" || manifest)
  bootstrap = g || c || 0x00 || epoch_le64 || pin || 137_le64        (Command2::Bootstrap payload)
  digest    = sha256("VISROOT2-COMMAND\\0" || bootstrap)
  fp        = sha256("TEST-S1-ENTRY\\0" || bootstrap)                  (TrustedEntry2 fixture stand-in)
  context   = g, c, installed_at (1,1,2), fp, digest, genesis, epoch, pin
Envelopes: magic VISADC01 | ver 1 | reserved 0 | context(188) | command digest(32) | signature(64),
signed with Ed25519 over "VISROOT2-ADMIN-COMMAND\\0" || envelope[0..232].
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
SEED = bytes([0x5A]) * 32
DOMAIN = b"VISROOT2-ADMIN-COMMAND\x00"
STATEMENT_DOMAIN = b"VISROOT2-ADMIN-STATEMENT\x00"
L = 2**252 + 27742317777372353535851937790883648493


def sha(*parts):
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.digest()


key = Ed25519PrivateKey.from_private_bytes(SEED)
public = key.public_key().public_bytes_raw()
manifest = bytearray(bytes.fromhex(SV["manifest_prime"]))
assert len(manifest) == 137
manifest[68:100] = public
manifest = bytes(manifest)
g, c = manifest[12:28], manifest[28:44]
epoch = struct.unpack("<Q", manifest[44:52])[0]
pin = sha(b"VISROOT2-ADMIN-MANIFEST\x00", manifest)
bootstrap = g + c + b"\x00" + struct.pack("<Q", epoch) + pin + struct.pack("<Q", 137)
digest = sha(b"VISROOT2-COMMAND\x00", bootstrap)
fp = sha(b"TEST-S1-ENTRY\x00", bootstrap)
genesis = bytes.fromhex(SV["genesis"])
context = {
    "generation": g, "cluster": c, "term": 1, "node": 1, "index": 2,
    "fingerprint": fp, "command": digest, "genesis": genesis, "epoch": epoch, "pin": pin,
}


def body(ctx, command_digest):
    return (b"VISADC01" + struct.pack("<HH", 1, 0) + ctx["generation"] + ctx["cluster"]
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

json.dump({
    "generator": "admin_command_reference.py",
    "provenance": {"python": sys.version.split()[0], "cryptography": cryptography.__version__,
                   "primitive": "cryptography Ed25519PrivateKey (RFC 8032 pure Ed25519)",
                   "inputs": ["statement_vectors.json"], "test_seed": SEED.hex()},
    "admin_public": public.hex(),
    "manifest": manifest.hex(),
    "pin": pin.hex(),
    "bootstrap_payload": bootstrap.hex(),
    "bootstrap_command": digest.hex(),
    "bootstrap_fingerprint": fp.hex(),
    "context": {k: (v.hex() if isinstance(v, bytes) else v) for k, v in context.items()},
    "digest_1": D1.hex(),
    "digest_2": D2.hex(),
    "valid": valid.hex(),
    "preimage": (DOMAIN + valid[:232]).hex(),
    "context_variants": variants,
    "statement_domain_signature": signed(context, D1, STATEMENT_DOMAIN).hex(),
    "noncanonical_s": noncanonical.hex() if noncanonical else None,
}, sys.stdout, indent=2)
sys.stdout.write("\n")
