#!/usr/bin/env python3
"""Independent reference for the M3 replica-tuple fixtures.

Run from this directory: python3 -I profile_two_replica_reference.py > profile_two_replica_vectors.json

Input: the M2 vectors' manifests (profile_two_inventory_vectors.json: "a" profile 1, "b"/"c"/"d"
profile 2). Snapshots (VISINV02) are packed from the published layout: 22-byte header (magic,
version u16, count u32, export seq u64), 320-byte entries (generation, cluster, pin, admin key,
state @96, tag @97 in version 2 else 0, zero 98..104, last seq u64, 208-byte record or zero),
sorted by (generation, cluster), SHA-256 trailer. Records (VISADR2) carry the entry's generation,
cluster, pin, the manifest's epoch, bootstrap (1,1,2), fingerprints A0/B0/C0 and the entry's last
seq. Pins are SHA-256("VISROOT2-ADMIN-MANIFEST\\0" || exact bytes).
"""
import hashlib
import json
import pathlib
import struct
import sys

HERE = pathlib.Path(__file__).resolve().parent
M = {k: bytes.fromhex(v["bytes"]) for k, v in
     json.loads((HERE / "profile_two_inventory_vectors.json").read_text())["manifests"].items()}
DOMAIN = b"VISROOT2-ADMIN-MANIFEST\x00"
STATE = {"Prepared": 1, "Bound": 2}


def pin(b):
    return hashlib.sha256(DOMAIN + b).digest()


def record(m, seq, p=None):
    p = p or pin(m)
    b = b"VISADR2\x00" + struct.pack("<HH", 1, 1) + m[12:28] + m[28:44] + p + m[44:52]
    b += struct.pack("<QIQ", 1, 1, 2) + bytes([0xA0]) * 32 + bytes([0xB0]) * 32 + bytes([0xC0]) * 32
    return b + struct.pack("<Q", seq)


def snapshot(entries, version):
    """entries: (manifest, state, last_seq, tag); tag is written only in version 2."""
    entries = sorted(entries, key=lambda e: e[0][12:44])
    b = b"VISINV02" + struct.pack("<HIQ", version, len(entries), max(e[2] for e in entries))
    for m, state, seq, tag in entries:
        b += m[12:28] + m[28:44] + pin(m) + m[68:100]
        b += bytes([STATE[state], tag if version == 2 else 0]) + bytes(6) + struct.pack("<Q", seq)
        b += record(m, seq) if state == "Bound" else bytes(208)
    return b + hashlib.sha256(b).digest()


def retag(b, index, tag):
    b = bytearray(b)
    b[22 + index * 320 + 97] = tag
    return bytes(b[:-32]) + hashlib.sha256(bytes(b[:-32])).digest()


a, bb, c = M["a"], M["b"], M["c"]
mixed_b_c = snapshot([(bb, "Bound", 2, 2), (c, "Prepared", 1, 2)], 2)
mixed_a_b = snapshot([(a, "Bound", 2, 1), (bb, "Prepared", 1, 2)], 2)
p2_short = bb[:137]                                   # profile 2 at 137 bytes
p1_long = bb[:10] + struct.pack("<H", 1) + bb[12:]    # profile 1 at 189 bytes
assert len(p2_short) == 137 and len(p1_long) == 189
out = {
    "generator": "profile_two_replica_reference.py",
    "provenance": {"python": sys.version.split()[0], "inputs": ["profile_two_inventory_vectors.json"]},
    "snapshots": {
        "selected_two": snapshot([(bb, "Bound", 2, 2)], 2).hex(),
        "mixed_a_b": mixed_a_b.hex(),
        "selected_one_v1": snapshot([(a, "Bound", 2, 1)], 1).hex(),
        "mixed_b_c": mixed_b_c.hex(),
        "mixed_b_c_selected_flipped": retag(mixed_b_c, 0, 1).hex(),
        "forged_v1_selected_two": snapshot([(bb, "Bound", 2, 1)], 1).hex(),
        "mixed_a_b_selected_one_tagged_two": retag(mixed_a_b, 0, 2).hex(),
    },
    "pairings": {
        "p2_at_137": {"bytes": p2_short.hex(), "exact_pin": pin(p2_short).hex()},
        "p1_at_189": {"bytes": p1_long.hex(), "exact_pin": pin(p1_long).hex()},
    },
}
json.dump(out, sys.stdout, indent=2)
sys.stdout.write("\n")
