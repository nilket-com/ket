#!/usr/bin/env python3
"""Independent profile-3 VISADS02 statement, install and gate vectors.

Run from this directory: python3 -I statement_reference.py [--out DIR]

It rewrites statement_vectors.json, gate_vectors.json, install_vectors.json,
root_profile_vectors.json and admission_command_vectors.json in place (with --out, it writes the
merged copies into DIR instead; the inputs are always read here). Every existing key is kept
byte for byte; only the `p3_` keys are (re)written, so a rerun is an empty diff.
Nothing is derived from the Rust code under test. Ed25519 comes from the `cryptography` package.

Published TEST seeds (public; they authorize nothing outside tests):
  - M3_PRIME, the profile-3 twin of the profile-1 G1d fixture manifest: admin 0x5A, governance id 0x6C / seed 0x6B,
    recovery id 0x7c / seed 0x7b; a statement by another signer uses seed 0x2F.
  - D-G3, used only for M3_S1, M3_KAT and M3_KAT_FOREIGN: admin 0xe1, governance 0xe2,
    recovery 0xe3, with role ids 0x51, 0x52 and 0x53.

Layouts (little endian):
  manifest  = "VISADM2\\0" | 1u16 | 3u16 | g16 | c16 | epoch u64 | admin id16 | admin key32 | 3u32
              | tenant u64 | scope u8 | table16 | schema u64 | gov id16 | gov key32 | 0x1FFu32
              | rec id16 | rec key32 | 0x1Fu32                                   (241 bytes)
  pin       = sha256("VISROOT2-ADMIN-MANIFEST\\0" || manifest)
  bootstrap = g || c || 0x00 || epoch u64 || pin || 241 u64        (Command2::Bootstrap payload)
  digest    = sha256("VISROOT2-COMMAND\\0" || bootstrap)
  fp        = sha256("TEST-S1-ENTRY\\0" || bootstrap)                 (TrustedEntry2 fixture stand-in)
  context   = g | c | term u64 | node u32 | index u64 | fp | digest | genesis | epoch u64 | pin
  statement = "VISADS02" | 1u16 | action u8 | 0u8 | context(188) | key(73) | signature(64), signed
              over "VISROOT2-ADMIN-STATEMENT\\0" || statement[0..273]
  record    = "VISADR2\\0" | 1u16 | 1u16 | g | c | pin | epoch | (term, node, index) | fp | digest
              | genesis | inventory_seq u64                                    (208 bytes)
  snapshot  = "VISINV02" | 3u16 | count u32 | export_seq u64 | per entry: g | c | pin | admin key
              | state u8 (2 Bound) | tag u8 (3) | 6 zero | last_seq u64 | record | sha256(prefix)
  command   = "VISEVT02" | 2u16 | 1u16 | payload length u32 | payload | sha256(prefix), with
              payload = g | c | 11 | statement (AdmissionPermission); digest = sha256(
              "VISROOT2-COMMAND\\0" || payload)
  root      = the gate's post-grant VISAUT04 root with the four context-bound fields replaced
              (tip fingerprint at 88, tip command digest at 120, control digest at 160, control
              length at 192) and the trailing sha256 checksum recomputed. The fixture-installed
              root with permissions takes only the M3_PRIME control digest and length 241.
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
AUTH = json.loads((HERE.parent / "auth" / "tests" / "vectors.json").read_text())
STATEMENT_DOMAIN = b"VISROOT2-ADMIN-STATEMENT\x00"


def sha(*parts):
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.digest()


def private(seed):
    return Ed25519PrivateKey.from_private_bytes(bytes([seed]) * 32)


def public(seed):
    return private(seed).public_key().public_bytes_raw()


def manifest(g, c, epoch, admin, namespace, governance, recovery):
    tenant, scope, table, schema = namespace
    b = (b"VISADM2\x00" + struct.pack("<HH", 1, 3) + g + c + struct.pack("<Q", epoch)
         + admin[0] + admin[1] + struct.pack("<I", 3)
         + struct.pack("<QB", tenant, scope) + table + struct.pack("<Q", schema)
         + governance[0] + governance[1] + struct.pack("<I", 0x1FF)
         + recovery[0] + recovery[1] + struct.pack("<I", 0x1F))
    assert len(b) == 241
    return b


class Root:
    """A root bootstrapped with `m` at (1, 1, 2) over `genesis`, through the fixture stand-in."""

    def __init__(self, m, genesis):
        self.manifest = m
        self.g, self.c = m[12:28], m[28:44]
        self.epoch = struct.unpack("<Q", m[44:52])[0]
        self.admin_id, self.admin_key = m[52:68], m[68:100]
        self.pin = sha(b"VISROOT2-ADMIN-MANIFEST\x00", m)
        self.bootstrap = (self.g + self.c + b"\x00" + struct.pack("<Q", self.epoch) + self.pin
                          + struct.pack("<Q", 241))
        self.command = sha(b"VISROOT2-COMMAND\x00", self.bootstrap)
        self.fp = sha(b"TEST-S1-ENTRY\x00", self.bootstrap)
        self.genesis = genesis
        self.log = (1, 1, 2)

    def context(self, **change):
        f = {"g": self.g, "c": self.c, "log": self.log, "fp": self.fp, "command": self.command,
             "genesis": self.genesis, "epoch": self.epoch, "pin": self.pin}
        f.update(change)
        return (f["g"] + f["c"] + struct.pack("<QIQ", *f["log"]) + f["fp"] + f["command"]
                + f["genesis"] + struct.pack("<Q", f["epoch"]) + f["pin"])

    def record(self, seq=1):
        return (b"VISADR2\x00" + struct.pack("<HH", 1, 1) + self.g + self.c + self.pin
                + struct.pack("<Q", self.epoch) + struct.pack("<QIQ", *self.log) + self.fp
                + self.command + self.genesis + struct.pack("<Q", seq))

    def snapshot(self, seq=1):
        body = (b"VISINV02" + struct.pack("<HIQ", 3, 1, seq) + self.g + self.c + self.pin
                + self.admin_key + bytes([2, 3]) + bytes(6) + struct.pack("<Q", seq)
                + self.record(seq))
        return body + sha(body)


def key(authority, revision, generation, tenant, scope, table, schema):
    return (authority + struct.pack("<Q", revision) + generation
            + struct.pack("<QB", tenant, scope) + table + struct.pack("<Q", schema))


def statement(action, context, k, seed):
    signed = b"VISADS02" + struct.pack("<HBB", 1, action, 0) + context + k
    assert len(signed) == 273
    return signed + private(seed).sign(STATEMENT_DOMAIN + signed)


def pair(b):
    return {"bytes": b.hex(), "preimage": (STATEMENT_DOMAIN + b[:273]).hex()}


def merge(name, fresh, indent):
    path = HERE / name
    text = path.read_text()
    newline = text.endswith("\n")
    old = json.loads(text)
    assert (json.dumps(old, indent=indent) + ("\n" if newline else "")) == text, name
    kept = {k: v for k, v in old.items() if not k.startswith("p3_")}
    kept.update(fresh)
    (OUT / name).write_text(json.dumps(kept, indent=indent) + ("\n" if newline else ""))


# ---- Inputs inlined, so a rerun is unchanged once the profile-1 keys are removed ----
# The profile-1 G1d fixture manifest's admission fields (statement_vectors.json "manifest_prime").
PRIME_FIELDS = dict(generation=bytes([0xA1]) * 16, cluster=bytes([0xC2]) * 16, epoch=7,
                    admin_id=bytes([0x0E]) * 16, namespace=(0, 1, bytes([0x7A]) * 16, 2))
# The gate's post-grant root over that manifest (gate_vectors.json "post_grant_root", registry 238).
GATE_ROOT = bytes.fromhex(
    "564953415554303404000100de010000a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1c2c2c2c2c2c2c2c2c2c2c2c2"
    "c2c2c2c201000000000000000100000002000000000000000100000000000000010000000400000000000000"
    "a59a83f1b19dad990b7e740b783889ab0f9c3a3e82e7ac597d9e22a2408bd8731ca0eabac988f0fb1bc4138f"
    "8f65cee7d1e09ae1e26d244c62aec1becb44445707000000000000009be107eb2d55f7f9c05944ec6498397d"
    "acd40e4cf379816e11d296c59642c4f789000000000000000000000000000000000000000000000000000000"
    "ee00000000000000000000000100000075000000414141414141414141414141414141410100000000000000"
    "07000000171717171717171717171717171717172b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b"
    "2b2b2b2b2b2b2b2b010000000000000001000000030000000000000000000000000000000000000000000000"
    "0000000000000000000100000071000000414141414141414141414141414141410100000000000000a1a1a1"
    "a1a1a1a1a1a1a1a1a1a1a1a1a10000000000000000017a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a020000000000"
    "0000010000000000000001000000040000000000000000000000000000000000000000000000000000000000"
    "000000000000000000002a4356747e22dd83d6f2cd606c1b5a54f1bf6d331eda7b424a02322fb4abebca"
)
# The fixture-installed root with permissions (root_profile_vectors.json "root_with_permissions").
PERMISSIONS_ROOT = bytes.fromhex(
    "56495341555430340400010053020000a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1c1c1c1c1c1c1c1c1c1c1c1c1"
    "c1c1c1c101000000000000000100000001000000000000000100000000000000010000000600000000000000"
    "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1d1d1d1d1d1d1d1d1d1d1d1d1"
    "d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d10700000000000000e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1"
    "e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e189000000000000000000000000000000000000000000000000000000"
    "6301000000000000000000000100000075000000414141414141414141414141414141410100000000000000"
    "07000000171717171717171717171717171717172b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b"
    "2b2b2b2b2b2b2b2b010000000000000001000000020000000000000000000000000000000000000000000000"
    "0000000000000000000200000071000000414141414141414141414141414141410100000000000000a1a1a1"
    "a1a1a1a1a1a1a1a1a1a1a1a1a105000000000000000000000000000000000000000000000000000000000000"
    "0000010000000000000001000000030000000000000001000000000000000100000005000000000000007100"
    "0000414141414141414141414141414141410100000000000000a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a10600"
    "0000000000000000000000000000000000000000000000000000000000000001000000000000000100000004"
    "0000000000000000000000000000000000000000000000000000000000000000000000000000004401c9a58a"
    "d9e9bc9c7eca9fe9085f71de5be1d6a77bb8353cd3a58473dcbe16"
)
PERMISSIONS_ROOT_FIELDS = {"length": 643, "registry": 355, "permission_coordinates": [3, 4, 5]}


def same_as_old(vectors, key, value):
    """While an old key is still present, the inlined input must equal it."""
    if key in vectors:
        assert vectors[key] == value, key


SV = json.loads((HERE / "statement_vectors.json").read_text())
GV = json.loads((HERE / "gate_vectors.json").read_text())
genesis = bytes.fromhex(SV["genesis"])
old_key = bytes.fromhex(SV["key"])

# ---- M3_PRIME: the profile-1 fixture's admission fields, profile 3, admin seed 0x5A ----
F = PRIME_FIELDS
if "manifest_prime" in SV:
    old = bytes.fromhex(SV["manifest_prime"])
    assert (old[12:28], old[28:44], old[52:68]) == (F["generation"], F["cluster"], F["admin_id"])
    assert struct.unpack("<Q", old[44:52])[0] == F["epoch"]
    assert old[104:137] == struct.pack("<QB", *F["namespace"][:2]) + F["namespace"][2] + \
        struct.pack("<Q", F["namespace"][3])
PRIME = manifest(
    F["generation"], F["cluster"], F["epoch"], (F["admin_id"], public(0x5A)), F["namespace"],
    (bytes([0x6C]) * 16, public(0x6B)),
    (bytes([0x7C]) * 16, public(0x7B)),
)
r = Root(PRIME, genesis)
ctx = r.context()
# The permission key is the old key: M3_PRIME keeps the fixture's generation and namespace.
assert old_key[24:40] == r.g and old_key[40:73] == PRIME[104:137]
grant = statement(1, ctx, old_key, 0x5A)
revoke = statement(2, ctx, old_key, 0x5A)
other = statement(1, ctx, old_key, 0x2F)
statements = {
    "p3_manifest": PRIME.hex(),
    "p3_manifest_pin": r.pin.hex(),
    "p3_admin_public": public(0x5A).hex(),
    "p3_other_public": public(0x2F).hex(),
    "p3_bootstrap": {"term": 1, "node": 1, "index": 2, "epoch": r.epoch, "length": 241},
    "p3_bootstrap_command": r.command.hex(),
    "p3_bootstrap_fingerprint_stand_in": r.fp.hex(),
    "p3_record": r.record().hex(),
    "p3_snapshot": r.snapshot().hex(),
    "p3_inventory_pin": sha(r.snapshot()).hex(),
    "p3_context": ctx.hex(),
    "p3_key": old_key.hex(),
    "p3_grant": pair(grant),
    "p3_revoke": pair(revoke),
    "p3_other_signer": pair(other),
}

# ---- D-G3: the KAT root on its own cluster and on the foreign cluster ----
kat_g, kat_c = bytes.fromhex(AUTH["generation"]), bytes.fromhex(AUTH["cluster"])
authority = bytes.fromhex(AUTH["authority_reference"])
assert len(authority) == 24
# The claim_c2 namespace (tenant 1, Raw, table 0, schema 0) and original lease epoch 1, inlined.
KAT_NAMESPACE = (1, 0, bytes(16), 0)
KAT_EPOCH = 1
for name, cluster in [("p3_kat_grant", kat_c), ("p3_kat_foreign_grant", bytes([0x34]) * 16)]:
    m = manifest(kat_g, cluster, KAT_EPOCH, (bytes([0x51]) * 16, public(0xE1)), KAT_NAMESPACE,
                 (bytes([0x52]) * 16, public(0xE2)), (bytes([0x53]) * 16, public(0xE3)))
    root = Root(m, bytes([0x11]) * 32)
    k = key(authority[:16], struct.unpack("<Q", authority[16:24])[0], kat_g, *KAT_NAMESPACE)
    statements[name] = pair(statement(1, root.context(), k, 0xE1))
    statements[name]["manifest_pin"] = root.pin.hex()
merge("statement_vectors.json", statements, 1)

# ---- Gate: re-signed variants, the post-grant tip and root ----
variants = {
    "installed_at": dict(log=(1, 1, 3)),
    "bootstrap_fingerprint": dict(fp=bytes([0x31]) * 32),
    "bootstrap_command": dict(command=bytes([0x32]) * 32),
    "genesis": dict(genesis=bytes([0x33]) * 32),
    "initial_epoch": dict(epoch=0x63),
    "manifest_pin": dict(pin=bytes([0x34]) * 32),
}
scope_key = old_key[:40] + struct.pack("<QB", 9, 0) + bytes(16) + struct.pack("<Q", 0)
revision_2 = old_key[:16] + struct.pack("<Q", 2) + old_key[24:]
assert revision_2.hex() == GV["revision_2_key"]
# The grant at (1, 1, 4) is the tip: AdmissionPermission(tag 11) payload under (g, c).
grant_payload = r.g + r.c + bytes([11]) + grant
tip_command = sha(b"VISROOT2-COMMAND\x00", grant_payload)
tip_fp = sha(b"TEST-S1-ENTRY\x00", grant_payload)
same_as_old(GV, "post_grant_root", {"length": len(GATE_ROOT), "hex": GATE_ROOT.hex(),
                                      "registry": 238})
assert GATE_ROOT[192:200] == struct.pack("<Q", 137)
root = bytearray(GATE_ROOT[:-32])
root[88:120], root[120:152], root[160:192] = tip_fp, tip_command, r.pin
root[192:200] = struct.pack("<Q", 241)
root = bytes(root) + sha(bytes(root))
merge("gate_vectors.json", {
    "p3_origin": {
        "genesis": genesis.hex(), "bootstrap_fingerprint": r.fp.hex(),
        "bootstrap_command": r.command.hex(), "manifest_pin": r.pin.hex(),
        "generation": r.g.hex(), "cluster": r.c.hex(), "initial_epoch": r.epoch,
        "bootstrap": [1, 1, 2],
    },
    "p3_context_variants": {
        name: statement(1, r.context(**change), old_key, 0x5A).hex()
        for name, change in variants.items()
    },
    "p3_scope_variant": statement(1, ctx, scope_key, 0x5A).hex(),
    "p3_revision_2": statement(1, ctx, revision_2, 0x5A).hex(),
    "p3_grant_tip": {"fingerprint": tip_fp.hex(), "command": tip_command.hex()},
    "p3_post_grant_root": {"length": len(root), "hex": root.hex(), "registry": 238},
}, 1)

# ---- Install: the same M3_PRIME root ----
merge("install_vectors.json", {
    "p3_bootstrap": {"term": 1, "node": 1, "index": 2, "epoch": r.epoch,
                     "control": r.pin.hex(), "length": 241},
    "p3_bootstrap_command": r.command.hex(),
    "p3_bootstrap_fingerprint_stand_in": r.fp.hex(),
    "p3_record": r.record().hex(),
    "p3_snapshot": r.snapshot().hex(),
    "p3_inventory_pin": sha(r.snapshot()).hex(),
    "p3_context": {
        "generation": r.g.hex(), "cluster": r.c.hex(), "installed_at": [1, 1, 2],
        "bootstrap_fingerprint": r.fp.hex(), "bootstrap_command": r.command.hex(),
        "genesis": genesis.hex(), "initial_epoch": r.epoch, "manifest_pin": r.pin.hex(),
    },
    "p3_admin_id": r.admin_id.hex(),
    "p3_admin_key": r.admin_key.hex(),
}, 1)

# ---- Root profile: the permission-section root, committed to M3_PRIME at length 241 ----
RP = json.loads((HERE / "root_profile_vectors.json").read_text())
same_as_old(RP, "root_with_permissions", dict(PERMISSIONS_ROOT_FIELDS, hex=PERMISSIONS_ROOT.hex(),
                                               sha256=sha(PERMISSIONS_ROOT).hex()))
assert PERMISSIONS_ROOT[:8] == b"VISAUT04" and PERMISSIONS_ROOT[16:32] == r.g
assert PERMISSIONS_ROOT[192:200] == struct.pack("<Q", 137)
root = bytearray(PERMISSIONS_ROOT[:-32])
root[160:192] = r.pin
root[192:200] = struct.pack("<Q", 241)
root = bytes(root) + sha(bytes(root))
fresh = {"length": 643, "hex": root.hex(), "sha256": sha(root).hex(), "registry": 355,
         "permission_coordinates": [3, 4, 5]}
merge("root_profile_vectors.json", {"p3_root_with_permissions": fresh}, 1)

# ---- Admission permission commands (tag 11) carrying the M3_PRIME statements ----
def command(g, c, stmt):
    payload = g + c + bytes([11]) + stmt
    body = b"VISEVT02" + struct.pack("<HHI", 2, 1, len(payload)) + payload
    return body + sha(body), sha(b"VISROOT2-COMMAND\x00", payload)


def resealed(b):
    return b[:-32] + sha(b[:-32])


AC = json.loads((HERE / "admission_command_vectors.json").read_text())
same_as_old(AC, "generation", r.g.hex())
same_as_old(AC, "cluster", r.c.hex())
commands = {}
for name, stmt in [("grant", grant), ("revoke", revoke), ("other_signer", other)]:
    b, d = command(r.g, r.c, stmt)
    commands[name] = {"bytes": b.hex(), "digest": d.hex()}
base = command(r.g, r.c, grant)[0]
refusals = {
    "envelope_generation_mismatch": resealed(base[:16] + bytes([0x99]) * 16 + base[32:]),
    "envelope_cluster_mismatch": resealed(base[:32] + bytes([0x98]) * 16 + base[48:]),
    "namespace_generation_mismatch": command(r.g, r.c, grant[:224] + bytes([0xB2]) * 16
                                             + grant[240:])[0],
    "short_statement": command(r.g, r.c, grant[:336])[0],
    "long_statement": command(r.g, r.c, grant + b"\x00")[0],
    "bad_statement_magic": command(r.g, r.c, b"X" + grant[1:])[0],
}
if "command_refusals" in AC:
    assert list(refusals) == list(AC["command_refusals"])
merge("admission_command_vectors.json", {
    "p3_commands": commands,
    "p3_command_refusals": {k: v.hex() for k, v in refusals.items()},
}, 1)
