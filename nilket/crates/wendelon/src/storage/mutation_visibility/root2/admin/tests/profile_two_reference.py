#!/usr/bin/env python3
"""Independent reference for profile-2 administrator manifest vectors (M1, additive codec).

Run from this directory: python3 -I profile_two_reference.py > profile_two_vectors.json

Input: the checked-in profile-1 golden manifests in vectors.json ("raw-tenant5", "table-tenant0").
Profile 2 = the profile-1 bytes with profile (bytes 10..12) := 2, then governance_id (16),
governance_key (32, Ed25519 public key of a fixed TEST seed) and governance_actions u32 = 0x1FF.
Digest = SHA-256("VISROOT2-ADMIN-MANIFEST\\0" || bytes). Error vectors are byte edits with the
expected error of the accepted 13-step decode order; multi-fault cases check precedence.
"""
import hashlib
import json
import pathlib
import struct
import sys

import cryptography
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

HERE = pathlib.Path(__file__).resolve().parent
P1 = {v["name"]: bytes.fromhex(v["bytes"]) for v in json.loads((HERE / "vectors.json").read_text())["valid"]}
NEG = json.loads((HERE / "vectors.json").read_text())["negative_keys"]
DOMAIN = b"VISROOT2-ADMIN-MANIFEST\x00"
GOV_SEED = bytes([0x6B]) * 32
GOV_ID = bytes([0x6C]) * 16
GOV_KEY = Ed25519PrivateKey.from_private_bytes(GOV_SEED).public_key().public_bytes_raw()


def two(p1, gov_id=GOV_ID, gov_key=GOV_KEY, mask=0x1FF):
    assert len(p1) == 137
    b = bytearray(p1)
    b[10:12] = struct.pack("<H", 2)
    return bytes(b) + gov_id + gov_key + struct.pack("<I", mask)


def edit(b, at, value):
    out = bytearray(b)
    out[at:at + len(value)] = value
    return bytes(out)


raw, table = two(P1["raw-tenant5"]), two(P1["table-tenant0"])
assert len(raw) == 189
valid = [{"name": n, "bytes": b.hex(), "digest": hashlib.sha256(DOMAIN + b).hexdigest(),
          "profile_one": P1[s].hex()}
         for n, b, s in [("raw", raw, "raw-tenant5"), ("table", table, "table-tenant0")]]

admin_id, admin_key = raw[52:68], raw[68:100]
bad_key = bytes.fromhex(NEG[0]["key"])          # an independent small-order key
z16, z8 = bytes(16), bytes(8)
cases = [
    ("over_read_bound", bytes(4097), "OverReadBound"),
    ("length_0", b"", "Length"),
    ("length_136", raw[:136], "Length"),
    ("length_190", raw + b"\x00", "Length"),
    ("magic", edit(raw, 0, b"X"), "Magic"),
    ("format_2", edit(raw, 8, struct.pack("<H", 2)), "UnsupportedFormat"),
    ("profile_3", edit(raw, 10, struct.pack("<H", 3)), "UnsupportedProfile"),
    ("profile_2_at_137", raw[:137], "ProfileLength"),
    ("profile_1_at_189", edit(raw, 10, struct.pack("<H", 1)), "ProfileLength"),
    ("profile_1_valid_137", P1["raw-tenant5"], "UnsupportedProfile"),
    ("zero_generation", edit(raw, 12, z16), "ZeroField"),
    ("zero_cluster", edit(raw, 28, z16), "ZeroField"),
    ("zero_epoch", edit(raw, 44, z8), "ZeroField"),
    ("zero_admin_id", edit(raw, 52, z16), "ZeroField"),
    ("zero_governance_id", edit(raw, 137, z16), "ZeroField"),
    ("admin_actions_7", edit(raw, 100, struct.pack("<I", 7)), "UnsupportedAction"),
    ("governance_actions_0xff", edit(raw, 185, struct.pack("<I", 0xFF)), "UnsupportedAction"),
    ("governance_actions_0x3ff", edit(raw, 185, struct.pack("<I", 0x3FF)), "UnsupportedAction"),
    ("scope_2", edit(raw, 112, b"\x02"), "Namespace"),
    ("raw_with_table", edit(raw, 113, bytes([7]) * 16), "Namespace"),
    ("table_zero_schema", edit(table, 129, z8), "Namespace"),
    ("id_reuse", edit(raw, 137, admin_id), "RoleReuse"),
    ("key_reuse", edit(raw, 153, admin_key), "RoleReuse"),
    ("admin_key_small_order", edit(raw, 68, bad_key), "NonCanonicalKey"),
    ("governance_key_small_order", edit(raw, 153, bad_key), "NonCanonicalKey"),
    # Multi-fault precedence.
    ("magic_and_profile_3", edit(edit(raw, 0, b"X"), 10, struct.pack("<H", 3)), "Magic"),
    ("format_and_profile_3", edit(edit(raw, 8, struct.pack("<H", 2)), 10, struct.pack("<H", 3)), "UnsupportedFormat"),
    ("profile_3_at_137", edit(raw[:137], 10, struct.pack("<H", 3)), "UnsupportedProfile"),
    ("zero_generation_and_bad_mask", edit(edit(raw, 12, z16), 185, struct.pack("<I", 1)), "ZeroField"),
    ("zero_governance_id_and_admin_actions", edit(edit(raw, 137, z16), 100, struct.pack("<I", 7)), "ZeroField"),
    ("bad_mask_and_scope_2", edit(edit(raw, 185, struct.pack("<I", 1)), 112, b"\x02"), "UnsupportedAction"),
    ("scope_2_and_key_reuse", edit(edit(raw, 112, b"\x02"), 153, admin_key), "Namespace"),
    ("id_reuse_and_bad_governance_key", edit(edit(raw, 137, admin_id), 153, bad_key), "RoleReuse"),
    ("both_keys_bad", edit(edit(raw, 68, bad_key), 153, bytes.fromhex(NEG[-1]["key"])), "NonCanonicalKey"),
]
json.dump({
    "generator": "profile_two_reference.py",
    "provenance": {"python": sys.version.split()[0], "cryptography": cryptography.__version__,
                   "inputs": ["vectors.json"], "governance_seed": GOV_SEED.hex()},
    "governance_id": GOV_ID.hex(),
    "governance_key": GOV_KEY.hex(),
    "valid": valid,
    "errors": [{"name": n, "bytes": b.hex(), "error": e} for n, b, e in cases],
}, sys.stdout, indent=2)
sys.stdout.write("\n")
