#!/usr/bin/env python3
"""Independent reference for M2 inventory events (version 2) and snapshots (version 2).

Run from this directory:
    python3 -I profile_two_inventory_reference.py > profile_two_inventory_vectors.json

Inputs: the checked-in profile-1 golden manifest "raw-tenant5" (../../admin/tests/vectors.json),
used only as a namespace template; every root below gets its own generation, cluster, admin id,
admin key (Ed25519 public key of a fixed TEST seed) and, for profile 2, governance id and key.

Layouts, from the published codecs at d6ce07b8 and the M2 contract (r1 sections 3-4):
  manifest p1 (137): magic VISADM2\\0, format u16=1 @8, profile u16 @10, generation 12..28,
    cluster 28..44, epoch u64 44..52, admin id 52..68, admin key 68..100, actions u32 @100,
    namespace 104..137. Profile 2 (189) = profile 2 @10, then governance id 16, key 32, mask u32.
  pin = SHA-256("VISROOT2-ADMIN-MANIFEST\\0" || the manifest's exact bytes), either profile.
  event (384): magic VISIEV02, version u16 @8 (2 iff a profile-2 Reserve), op @10, reserved @11,
    seq u64 @12, op id 20..36, identity 36..132 (generation, cluster, pin, admin key; zero for
    Repair), payload 132..340, reserved 340..352, SHA-256(bytes[..352]) at 352.
  record/observation (208): magic VISADR2\\0 / VISAOB2\\0, u16 1, u16 1, generation, cluster, pin,
    epoch u64, bootstrap log (term u64, node u32, index u64), bootstrap fingerprint, command,
    genesis, inventory seq u64 (0 for an observation).
  snapshot: magic VISINV02, version u16 (2 iff any entry is profile 2), count u32, export seq u64;
    entries of 320 (identity 96, state @96, tag @97 in v2 else 0, zero 98..104, last seq u64,
    record 208 or zero), ordered by (generation, cluster); SHA-256 trailer over the rest.
"""
import hashlib
import json
import pathlib
import struct
import sys

import cryptography
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

HERE = pathlib.Path(__file__).resolve().parent
ADMIN = HERE.parent.parent / "admin" / "tests" / "vectors.json"
TEMPLATE = {v["name"]: bytes.fromhex(v["bytes"]) for v in json.loads(ADMIN.read_text())["valid"]}[
    "raw-tenant5"]
DOMAIN = b"VISROOT2-ADMIN-MANIFEST\x00"
RESERVE, APPROVE, CONFLICT, RETIRE, REPAIR = 1, 2, 3, 4, 5
STATE = {"Prepared": 1, "Bound": 2, "Conflicted": 3, "Retired": 4}


def key(seed):
    return Ed25519PrivateKey.from_private_bytes(bytes([seed]) * 32).public_key().public_bytes_raw()


def edit(b, at, value):
    out = bytearray(b)
    out[at:at + len(value)] = value
    return bytes(out)


def manifest(tag, governance=None, mask=0x1FF):
    """A root's manifest; every field derived from one tag byte, so roots share nothing."""
    m = TEMPLATE
    m = edit(m, 12, bytes([tag]) * 16)
    m = edit(m, 28, bytes([tag ^ 0x0F]) * 16)
    m = edit(m, 44, struct.pack("<Q", 0x100 + tag))
    m = edit(m, 52, bytes([tag ^ 0x33]) * 16)
    m = edit(m, 68, key(tag))
    if governance is None:
        return m
    gid, gseed = governance
    return edit(m, 10, struct.pack("<H", 2)) + bytes([gid]) * 16 + key(gseed) + struct.pack("<I", mask)


def pin(m):
    return hashlib.sha256(DOMAIN + m).digest()


def identity(m):
    return m[12:28] + m[28:44] + pin(m) + m[68:100]


def op_id(n):
    return bytes([n]) * 15 + b"\x0e"


def event(version, op, seq, oid, ident, payload):
    b = b"VISIEV02" + struct.pack("<HBB", version, op, 0) + struct.pack("<Q", seq) + oid
    b += ident.ljust(96, b"\x00") + payload.ljust(208, b"\x00") + bytes(12)
    assert len(b) == 352
    return b + hashlib.sha256(b).digest()


def reseal(b):
    return b[:352] + hashlib.sha256(b[:352]).digest()


def record(magic, m, inventory_seq):
    b = magic + struct.pack("<HH", 1, 1) + m[12:28] + m[28:44] + pin(m) + m[44:52]
    b += struct.pack("<QIQ", 7, 3, 40 + m[12]) + bytes([0xA0]) * 32 + bytes([0xB0]) * 32
    b += bytes([0xC0]) * 32 + struct.pack("<Q", inventory_seq)
    assert len(b) == 208
    return b


def reserve(seq, oid, m):
    return event(2 if len(m) == 189 else 1, RESERVE, seq, oid, identity(m), m)


# Roots: A profile 1, B and C profile 2, D profile 2 (profile-2-only log).
A = manifest(0x21)
B = manifest(0x42, governance=(0x91, 0x92))
C = manifest(0x63, governance=(0x93, 0x94))
D = manifest(0x84, governance=(0x95, 0x96))
for x, y in [(A, B), (A, C), (B, C), (A, D), (B, D), (C, D)]:
    assert all(x[i:j] != y[i:j] for i, j in [(12, 28), (28, 44), (68, 100)]) and pin(x) != pin(y)
assert len(A) == 137 and len(B) == len(C) == len(D) == 189

# The mixed log: v1 Reserve, v2 Reserve x2, v1 Approve/Conflict/Retire/Repair (generated valid
# transitions), and the expected state after each step.
obs_c = record(b"VISAOB2\x00", edit(C, 44, struct.pack("<Q", 1)), 0)  # epoch differs: Conflict
mixed = [
    reserve(1, op_id(1), A),
    reserve(2, op_id(2), B),
    reserve(3, op_id(3), C),
    event(1, APPROVE, 4, op_id(4), identity(A), record(b"VISADR2\x00", A, 4)),
    event(1, APPROVE, 5, op_id(5), identity(B), record(b"VISADR2\x00", B, 5)),
    event(1, CONFLICT, 6, op_id(6), identity(C), obs_c),
    event(1, RETIRE, 7, op_id(7), identity(A), b""),
    event(1, REPAIR, 8, op_id(8), b"",
          bytes([0x77]) * 32 + struct.pack("<QQI", 7 * 384 + 10, 7 * 384, 10)),
]
assert [e[8] for e in mixed] == [1, 2, 2, 1, 1, 1, 1, 1]
log = b"".join(mixed)


def snapshot(version, export_seq, entries):
    b = b"VISINV02" + struct.pack("<HIQ", version, len(entries), export_seq)
    for m, state, last_seq, rec, tag in sorted(entries, key=lambda e: e[0][12:44]):
        b += identity(m) + bytes([STATE[state], tag]) + bytes(6) + struct.pack("<Q", last_seq)
        b += rec if rec else bytes(208)
    return b + hashlib.sha256(b).digest()


def reseal_snapshot(b):
    return b[:-32] + hashlib.sha256(b[:-32]).digest()


mixed_entries = [
    (A, "Retired", 7, None, 1),
    (B, "Bound", 5, record(b"VISADR2\x00", B, 5), 2),
    (C, "Conflicted", 6, None, 2),
]
snap_mixed = snapshot(2, 8, mixed_entries)
log_two = reserve(1, op_id(0x31), D) + event(1, APPROVE, 2, op_id(0x32), identity(D),
                                              record(b"VISADR2\x00", D, 2))
snap_two = snapshot(2, 2, [(D, "Bound", 2, record(b"VISADR2\x00", D, 2), 2)])
log_one = reserve(1, op_id(0x41), A)
snap_one = snapshot(1, 1, [(A, "Prepared", 1, None, 0)])

# Pin binding: B' shares B's Admission fields (generation, cluster, admin key) but not its
# governance id or key; the pins differ, so B' is a distinct root that shares components.
B_gid = manifest(0x42, governance=(0x97, 0x92))
B_gkey = manifest(0x42, governance=(0x91, 0x98))
A_like_B = B[:137]
A_like_B = edit(A_like_B, 10, struct.pack("<H", 1))
assert len({pin(B), pin(B_gid), pin(B_gkey), pin(A_like_B)}) == 4
assert B[:10] + B[12:137] == A_like_B[:10] + A_like_B[12:137]

# Event refusals: (name, bytes, reason). Multi-fault cases pin precedence.
v2 = mixed[1]
v1_approve = mixed[3]
p1_in_v2 = event(2, RESERVE, 1, op_id(1), identity(A), A)
p2_in_v1 = event(1, RESERVE, 2, op_id(2), identity(B), B)
event_errors = [
    ("v3", reseal(edit(v2, 8, struct.pack("<H", 3))), "Version"),
    ("v0", reseal(edit(v2, 8, struct.pack("<H", 0))), "Version"),
    ("v2_approve", reseal(edit(v1_approve, 8, struct.pack("<H", 2))), "Version"),
    ("v2_conflict", reseal(edit(mixed[5], 8, struct.pack("<H", 2))), "Version"),
    ("v2_retire", reseal(edit(mixed[6], 8, struct.pack("<H", 2))), "Version"),
    ("v2_repair", reseal(edit(mixed[7], 8, struct.pack("<H", 2))), "Version"),
    ("v2_profile_one_payload", p1_in_v2, "Version"),
    ("v2_tail_after_189", reseal(edit(v2, 132 + 189, b"\x01")), "Payload"),
    ("v2_last_payload_byte", reseal(edit(v2, 339, b"\x01")), "Payload"),
    ("v2_bad_mask", reseal(edit(v2, 132 + 185, struct.pack("<I", 0xFF))), "Payload"),
    ("v2_manifest_magic", reseal(edit(v2, 132, b"X")), "Payload"),
    ("v2_manifest_profile_3", reseal(edit(v2, 142, struct.pack("<H", 3))), "Payload"),
    ("v2_manifest_format_2", reseal(edit(v2, 140, struct.pack("<H", 2))), "Payload"),
    ("v2_role_reuse", reseal(edit(v2, 132 + 137, B[52:68])), "Payload"),
    ("v1_profile_two_payload", p2_in_v1, "Payload"),
    # Precedence.
    ("v2_bad_checksum_and_v3", edit(edit(v2, 8, struct.pack("<H", 3)), 383, b"\x00"), "Checksum"),
    ("v2_magic_and_v3", reseal(edit(edit(v2, 0, b"X"), 8, struct.pack("<H", 3))), "Magic"),
    ("v3_and_unknown_op", reseal(edit(edit(v2, 8, struct.pack("<H", 3)), 10, b"\x09")), "Version"),
    ("v2_unknown_op", reseal(edit(v2, 10, b"\x09")), "UnknownOp"),
    ("v2_approve_and_reserved", reseal(edit(edit(v1_approve, 8, struct.pack("<H", 2)), 11, b"\x01")),
     "Version"),
    ("v2_reserved_and_bad_payload", reseal(edit(edit(v2, 11, b"\x01"), 132, b"X")), "Reserved"),
    ("v2_zero_op_id", reseal(edit(v2, 20, bytes(16))), "ZeroOpId"),
    ("v2_zero_header_and_profile_one", reseal(edit(p1_in_v2, 36, bytes(16))), "Header"),
    ("v2_profile_one_and_tail", reseal(edit(p1_in_v2, 132 + 189, b"\x01")), "Payload"),
]

# Snapshot refusals.
empty_v2 = reseal_snapshot(edit(snapshot(1, 0, []), 8, struct.pack("<H", 2)))
all_one_v2 = snapshot(2, 1, [(A, "Prepared", 1, None, 1)])
E0 = 22          # first entry
E1 = 22 + 320    # second entry
snapshot_errors = [
    ("v2_empty", empty_v2, "NotCanonical"),
    ("v2_all_profile_one", all_one_v2, "NotCanonical"),
    ("v3", reseal_snapshot(edit(snap_mixed, 8, struct.pack("<H", 3))), "Version"),
    ("v0", reseal_snapshot(edit(snap_mixed, 8, struct.pack("<H", 0))), "Version"),
    ("tag_0", reseal_snapshot(edit(snap_mixed, E0 + 97, b"\x00")), "ProfileTag"),
    ("tag_3", reseal_snapshot(edit(snap_mixed, E1 + 97, b"\x03")), "ProfileTag"),
    ("tag_255", reseal_snapshot(edit(snap_two, E0 + 97, b"\xff")), "ProfileTag"),
    ("v1_with_tag_1", reseal_snapshot(edit(snap_one, E0 + 97, b"\x01")), "Reserved"),
    ("v1_with_tag_2", reseal_snapshot(edit(snap_one, E0 + 97, b"\x02")), "Reserved"),
    ("v2_reserved_98", reseal_snapshot(edit(snap_mixed, E0 + 98, b"\x01")), "Reserved"),
    ("v2_reserved_103", reseal_snapshot(edit(snap_mixed, E1 + 103, b"\x01")), "Reserved"),
    # A tag flipped to the other valid value is locally well-formed; canonicity cannot see it.
    ("v2_mixed_tag_flipped_to_one", reseal_snapshot(edit(snap_mixed, E1 + 97, b"\x01")), None),
    # Precedence.
    ("v3_bad_trailer", edit(edit(snap_mixed, 8, struct.pack("<H", 3)), len(snap_mixed) - 1, b"\x00"),
     "Version"),
    ("v2_bad_trailer_and_tag_3", edit(edit(snap_mixed, E0 + 97, b"\x03"), len(snap_mixed) - 1,
                                      b"\x00"), "Trailer"),
    ("v2_tag_3_and_reserved", reseal_snapshot(edit(edit(snap_mixed, E0 + 97, b"\x03"), E0 + 98, b"\x01")),
     "ProfileTag"),
    ("v2_tag_0_and_bad_state", reseal_snapshot(edit(edit(snap_mixed, E0 + 97, b"\x00"), E0 + 96, b"\x09")),
     "ProfileTag"),
    ("v2_reserved_and_bad_state", reseal_snapshot(edit(edit(snap_mixed, E0 + 98, b"\x01"), E0 + 96, b"\x09")),
     "Reserved"),
    ("v2_entry0_bad_state_entry1_tag_3",
     reseal_snapshot(edit(edit(snap_mixed, E0 + 96, b"\x09"), E1 + 97, b"\x03")), "State"),
    ("v2_count_short", reseal_snapshot(edit(snap_mixed, 10, struct.pack("<I", 2))), "Length"),
]
# The flipped tag decodes; it then re-encodes with the flipped tag and stays canonical, because
# decode never sees a manifest. Recorded so the test pins that decode makes no manifest claim.

json.dump({
    "generator": "profile_two_inventory_reference.py",
    "provenance": {"python": sys.version.split()[0], "cryptography": cryptography.__version__,
                   "inputs": ["../../admin/tests/vectors.json (raw-tenant5, template only)"],
                   "seeds": "admin key = Ed25519(seed [tag]*32); governance key = Ed25519(seed [g]*32)"},
    "manifests": {n: {"bytes": m.hex(), "pin": pin(m).hex()} for n, m in [
        ("a", A), ("b", B), ("c", C), ("d", D), ("b_governance_id", B_gid),
        ("b_governance_key", B_gkey), ("b_admission_as_profile_one", A_like_B)]},
    "mixed_log": [e.hex() for e in mixed],
    "mixed_snapshot": snap_mixed.hex(),
    "two_log": log_two.hex(),
    "two_snapshot": snap_two.hex(),
    "one_log": log_one.hex(),
    "one_snapshot": snap_one.hex(),
    "event_errors": [{"name": n, "bytes": b.hex(), "reason": r} for n, b, r in event_errors],
    "snapshot_errors": [{"name": n, "bytes": b.hex(), "error": r} for n, b, r in snapshot_errors],
}, sys.stdout, indent=2)
sys.stdout.write("\n")
