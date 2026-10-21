#!/usr/bin/env python3
"""Independent profile-3 provisioning vectors: VISOEV02 events, v3/v4 snapshots, the pinned tuple
and VISREP04 repair evidence over two profile-3 roots.

Run from this directory: python3 -I provisioning_reference.py [--out DIR]

It merges `p3_` keys into anchor_vectors.json, inventory_vectors.json and owner_vectors.json in
place (with --out, it writes the merged copies into DIR instead; inputs are always read here).
Every existing key is kept byte for byte, so a rerun is an empty diff. Nothing is derived from the
Rust code under test. Ed25519 comes from the `cryptography` package.

Roots (profile 3, 241 bytes; TEST seeds are public and authorize nothing):
  - m1 = M3_PRIME: generation 0xa1, cluster 0xc2, epoch 7, admin id 0x0e, tenant 0, Table 0x7a,
    schema 2; admin seed 0x5A, governance 0x6C/0x6B, recovery 0x7c/0x7b (inlined bytes).
  - m2: the profile-3 twin of the profile-1 anchor "m2" admission fields (generation 0xb3, cluster
    0xd4, epoch 3, admin id 0x0f, tenant 9, Raw); admin seed 0x2C, governance 0x6D/0x2D,
    recovery 0x7d/0x2E.

Layouts (little endian):
  event     = "VISOEV02" | 1u16 | op u8 | 0u8 | seq u64 | op id16 | identity96 (zero for Repair)
              | payload length u16 | 10 zero | payload, zero-filled to byte 480 | sha256(event[..480])
  record    = "VISADR2\\0" | 1u16 | 1u16 | g | c | pin | epoch | (term u64, node u32, index u64) | fp
              | command | genesis | inventory_seq u64                                      (208)
  observ.   = the record with magic "VISAOB2\\0" and the last 8 bytes zero                 (208)
  terminal  = log | fp | command | state | sha256("VISROOT2-RETIREMENT-OBSERVATION3\\0" ||
              observation(record) || admin key || log || fp || command || state) | seq      (156)
  repair    = tail sha256 | original_len u64 | boundary u64 | tail_len u32                 (52)
  snapshot  = "VISINV02" | version u16 (4 iff a terminal, else 3) | count u32 | export_seq u64 |
              per entry: identity96 | state u8 | tag 3 | 6 zero | last_seq u64 | record, or
              terminal + 52 zero, or 208 zero | sha256(prefix)
  evidence  = "VISREP04" | 1u16 | 2u16 | prefix sha256 | boundary u64 | original_len u64 |
              tail_len u32 | 0u32 | tail sha256 | op id16 | repair seq u64 | tail | sha256(prefix)
              with context = sha256("VISROOT2-REPAIR-CONTEXT-V2\\0" || prefix sha256 || boundary
              || original_len || tail sha256) and op id = sha256("VISROOT2-REPAIR-OP-V2\\0" ||
              context)[..16]; the file name is hex(context) + ".rep".
"""
import argparse
import hashlib
import json
import pathlib
import struct

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

HERE = pathlib.Path(__file__).resolve().parent
ARGS = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ARGS.add_argument("--out", type=pathlib.Path, default=HERE,
                  help="directory for the merged vector files (default: beside this script)")
OUT = ARGS.parse_args().out
EVENT = 512


def sha(*parts):
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.digest()


def public(seed):
    return Ed25519PrivateKey.from_private_bytes(bytes([seed]) * 32).public_key().public_bytes_raw()


def manifest(g, c, epoch, admin_id, admin_seed, namespace, governance, recovery):
    tenant, scope, table, schema = namespace
    b = (b"VISADM2\x00" + struct.pack("<HH", 1, 3) + g + c + struct.pack("<Q", epoch) + admin_id
         + public(admin_seed) + struct.pack("<I", 3) + struct.pack("<QB", tenant, scope) + table
         + struct.pack("<Q", schema) + governance[0] + public(governance[1])
         + struct.pack("<I", 0x1FF) + recovery[0] + public(recovery[1]) + struct.pack("<I", 0x1F))
    assert len(b) == 241
    return b


M1 = bytes.fromhex(
    "56495341444d320001000300a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2"
    "07000000000000000e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0d7550754e0800a5d237eef5826035766b9b3e5a"
    "15868a940ab289958788e3b0030000000000000000000000017a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a020000"
    "00000000006c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c8320a51977d8c38ca8a4927c670df5821e449761945e15"
    "e9efb26a1509d230eaff0100007c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7f747b67bd3fe63c2a736739dfe401"
    "56d622347346e70f68f51c178a75ce55371f000000"
)
# M3_PRIME is the constructor build of its published inputs.
assert M1 == manifest(bytes([0xA1]) * 16, bytes([0xC2]) * 16, 7, bytes([0x0E]) * 16, 0x5A,
                      (0, 1, bytes([0x7A]) * 16, 2), (bytes([0x6C]) * 16, 0x6B),
                      (bytes([0x7C]) * 16, 0x7B))
M2 = manifest(bytes([0xB3]) * 16, bytes([0xD4]) * 16, 3, bytes([0x0F]) * 16, 0x2C,
              (9, 0, bytes(16), 0), (bytes([0x6D]) * 16, 0x2D), (bytes([0x7D]) * 16, 0x2E))


def pin(m):
    return sha(b"VISROOT2-ADMIN-MANIFEST\x00", m)


def identity(m):
    return m[12:44] + pin(m) + m[68:100]


def epoch(m):
    return struct.unpack("<Q", m[44:52])[0]


def record(m, seq, magic=b"VISADR2\x00"):
    b = (magic + struct.pack("<HH", 1, 1) + m[12:44] + pin(m) + m[44:52]
         + struct.pack("<QIQ", 1, 3, 2) + bytes([0xB0]) * 32 + bytes([0xB1]) * 32
         + bytes([0x9E]) * 32 + struct.pack("<Q", seq))
    assert len(b) == 208
    return b


def observation(rec):
    return b"VISAOB2\x00" + rec[8:200] + bytes(8)


def event(op, seq, op_id, ident, payload):
    b = (b"VISOEV02" + struct.pack("<HBBQ", 1, op, 0, seq) + op_id + ident.ljust(96, b"\x00")
         + struct.pack("<H", len(payload)) + bytes(10) + payload.ljust(336, b"\x00"))
    assert len(b) == 480
    return b + sha(b)


def terminal(m, rec, seq):
    log = struct.pack("<QIQ", 1, 1, 9)
    fp, command, state = bytes([0x19]) * 32, bytes([0x1A]) * 32, bytes([0x1B]) * 32
    full = observation(rec) + m[68:100] + log + fp + command + state
    t = log + fp + command + state + sha(b"VISROOT2-RETIREMENT-OBSERVATION3\x00", full) \
        + struct.pack("<Q", seq)
    assert len(t) == 156
    return t


def repair_payload(seq, tail_len, tail_sha):
    boundary = (seq - 1) * EVENT
    return tail_sha + struct.pack("<QQI", boundary + tail_len, boundary, tail_len)


def entry(m, state, last_seq, rec=None, term=None):
    body = rec if rec is not None else (term + bytes(52) if term is not None else bytes(208))
    return identity(m) + bytes([state, 3]) + bytes(6) + struct.pack("<Q", last_seq) + body


def snapshot(export_seq, entries, terminal_present=False):
    b = b"VISINV02" + struct.pack("<HIQ", 4 if terminal_present else 3, len(entries), export_seq)
    for e in entries:
        b += e
    return b + sha(b)


def op(n):
    return bytes([n]) * 16


approve_record = record(M1, 2)
conflict_obs = observation(record(M2, 0))[:44] + bytes([0x5E]) * 32 + observation(record(M2, 0))[76:]
retire_terminal = terminal(M1, approve_record, 5)
events = {
    "reserve": event(1, 1, op(1), identity(M1), M1),
    "approve": event(2, 2, op(2), identity(M1), approve_record),
    "reserve_second": event(1, 3, op(3), identity(M2), M2),
    "conflict": event(3, 4, op(4), identity(M2), conflict_obs),
    "retire": event(4, 5, op(5), identity(M1), retire_terminal),
    "repair": event(5, 6, op(6), b"", repair_payload(6, 100, bytes([0x77]) * 32)),
}
log = b"".join(events.values())
snapshots = {
    "prepared": snapshot(1, [entry(M1, 1, 1)]),
    "bound": snapshot(2, [entry(M1, 2, 2, rec=approve_record)]),
    "full": snapshot(6, [entry(M1, 4, 5, term=retire_terminal), entry(M2, 3, 4)], True),
}


def merge(name, fresh):
    path = HERE / name
    text = path.read_text()
    newline = text.endswith("\n")
    old = json.loads(text)
    assert json.dumps(old, indent=1) + ("\n" if newline else "") == text, name
    kept = {k: v for k, v in old.items() if not k.startswith("p3_")}
    kept.update(fresh)
    (OUT / name).write_text(json.dumps(kept, indent=1) + ("\n" if newline else ""))


merge("inventory_vectors.json", {
    "p3_events": {k: v.hex() for k, v in events.items()},
    "p3_log": log.hex(),
    "p3_manifest_second": M2.hex(),
    "p3_snapshots": {k: v.hex() for k, v in snapshots.items()},
})

f1 = M1  # the Bound tuple root
merge("anchor_vectors.json", {
    "p3_manifests": {name: {"bytes": m.hex(), "pin": pin(m).hex()}
                     for name, m in [("m1", M1), ("m2", M2)]},
    "p3_snapshots": {name: {"bytes": snapshots[name].hex(), "pin": sha(snapshots[name]).hex(),
                            "export_seq": seq}
                     for name, seq in [("prepared", 1), ("bound", 2), ("full", 6)]},
    "p3_bound_m1": {
        "manifest": f1.hex(), "manifest_pin": pin(f1).hex(),
        "inventory_pin": sha(snapshots["bound"]).hex(), "export_seq": 2, "entry_last_seq": 2,
        "record": approve_record.hex(),
        "fields": {"generation": f1[12:28].hex(), "cluster": f1[28:44].hex(), "initial_epoch": 7,
                   "admin_id": f1[52:68].hex(), "admin_key": f1[68:100].hex(), "tenant": 0,
                   "scope": 1, "table": f1[113:129].hex(), "schema": 2},
    },
})


def case(prefix, tail):
    boundary = len(prefix)
    original = boundary + len(tail)
    context = sha(b"VISROOT2-REPAIR-CONTEXT-V2\x00", sha(prefix),
                  struct.pack("<QQ", boundary, original), sha(tail))
    op_id = sha(b"VISROOT2-REPAIR-OP-V2\x00", context)[:16]
    seq = boundary // EVENT + 1
    evidence = (b"VISREP04" + struct.pack("<HH", 1, 2) + sha(prefix)
                + struct.pack("<QQI", boundary, original, len(tail)) + bytes(4) + sha(tail)
                + op_id + struct.pack("<Q", seq) + tail)
    evidence += sha(evidence)
    expected = event(5, seq, op_id, b"", repair_payload(seq, len(tail), sha(tail)))
    return {"log": (prefix + tail).hex(), "boundary": boundary, "context": context.hex(),
            "op_id": op_id.hex(), "name": context.hex() + ".rep", "evidence": evidence.hex(),
            "expected_repair": expected.hex()}


OV = json.loads((HERE / "owner_vectors.json").read_text())
tail = bytes.fromhex(OV["tail"])
assert len(tail) == 100
golden = list(events.values())
other_prefix = [
    event(1, 1, op(0x90), identity(M1), M1),
    event(2, 2, op(0x91), identity(M1), approve_record),
    event(1, 3, op(0x92), identity(M2), M2),
]
merge("owner_vectors.json", {
    "p3_cases": {
        "base": case(b"".join(golden[:3]), tail),
        "same_boundary_other_prefix": case(b"".join(other_prefix), tail),
        "other_boundary": case(b"".join(golden[:5]), tail),
    },
})
