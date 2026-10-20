#!/usr/bin/env python3
"""Independent reference for the stage C verification vectors.

Run from this directory: python3 -I verification_reference.py > verification_vectors.json

Inputs are existing checked-in goldens only (auth/tests/vectors.json, statement_vectors.json);
nothing is derived from the Rust code under test. Ed25519 comes from the `cryptography` package.

1. Re-signs the golden admission and holder preimages with the public RFC 8032 TEST 1 seed already
   recorded in auth/tests/vectors.json, and records the signatures.
2. Rebuilds the AdmissionPermission statement preimage (domain + the 273 signed bytes) from the
   `grant` statement and independently verifies its signature under the administrator public key.
   The administrator seed is not in the repository, so the statement is verified, not re-signed.
"""
import json
import pathlib
import sys

import cryptography
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

HERE = pathlib.Path(__file__).resolve().parent
AUTH = json.loads((HERE.parent / "auth" / "tests" / "vectors.json").read_text())
STATEMENT = json.loads((HERE / "statement_vectors.json").read_text())
DOMAIN = b"VISROOT2-ADMIN-STATEMENT\x00"
STATEMENT_BYTES = 337
SIGNED = STATEMENT_BYTES - 64


def unhex(s):
    return bytes.fromhex(s)


seed = unhex(AUTH["public_test_signing_seed"])
key = Ed25519PrivateKey.from_private_bytes(seed)
public = key.public_key().public_bytes_raw()
assert public == unhex(AUTH["public_key"]), "seed does not match the recorded public key"

admission_preimage = unhex(AUTH["admission_preimage"])
holder_preimage = unhex(AUTH["holder_preimage"])
admission_signature = key.sign(admission_preimage)
holder_signature = key.sign(holder_preimage)

statement = unhex(STATEMENT["grant"]["bytes"])
assert len(statement) == STATEMENT_BYTES
statement_preimage = DOMAIN + statement[:SIGNED]
admin_public = unhex(STATEMENT["admin_public"])
Ed25519PublicKey.from_public_bytes(admin_public).verify(statement[SIGNED:], statement_preimage)

# The profile-3 (M3_PRIME) grant, verified the same way under its published administrator key.
p3_statement = unhex(STATEMENT["p3_grant"]["bytes"])
assert len(p3_statement) == STATEMENT_BYTES
p3_preimage = DOMAIN + p3_statement[:SIGNED]
p3_admin_public = unhex(STATEMENT["p3_admin_public"])
Ed25519PublicKey.from_public_bytes(p3_admin_public).verify(p3_statement[SIGNED:], p3_preimage)

json.dump(
    {
        "generator": "verification_reference.py",
        "provenance": {
            "python": sys.version.split()[0],
            "cryptography": cryptography.__version__,
            "primitive": "cryptography Ed25519PrivateKey / Ed25519PublicKey (RFC 8032 pure Ed25519)",
            "inputs": ["auth/tests/vectors.json", "statement_vectors.json"],
        },
        "public_key": public.hex(),
        "admission_preimage": admission_preimage.hex(),
        "admission_signature": admission_signature.hex(),
        "holder_preimage": holder_preimage.hex(),
        "holder_signature": holder_signature.hex(),
        "statement_preimage": statement_preimage.hex(),
        "statement_signature": statement[SIGNED:].hex(),
        "statement_admin_public": admin_public.hex(),
        "statement_verified": True,
        "p3_statement_preimage": p3_preimage.hex(),
        "p3_statement_signature": p3_statement[SIGNED:].hex(),
        "p3_statement_admin_public": p3_admin_public.hex(),
        "p3_statement_verified": True,
    },
    sys.stdout,
    indent=2,
)
sys.stdout.write("\n")
