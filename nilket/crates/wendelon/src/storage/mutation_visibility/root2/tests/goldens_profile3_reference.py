#!/usr/bin/env python3
"""Independent profile-3 S1 golden family (D-G1): goldens.json twins and the origin p3_ keys.

Run from this directory: python3 -I goldens_profile3_reference.py [--out DIR]

It rewrites goldens.json and origin_vectors.json in place (with --out, it writes the merged copies
into DIR instead; the inputs are always read here). In goldens.json it replaces, in place and under
the same names, the S1 entries of the parent lineage and the nine roots, appends
command_bootstrap_p3, command_admission_permission_p3 and result_permission_p3, and writes the
"provenance" map (fixture -> history step) that T-FAMILY replays. Every other entry is kept byte for
byte. In origin_vectors.json only the p3_ keys are (re)written. A rerun is an empty diff.
Nothing is derived from the Rust code under test. Ed25519 comes from the `cryptography` package.

Sources (no entry is read from the file it rewrites):
  - the nine pre-twin roots: removed_forms.json N13 (its digest is pinned by the plan), which carry
    every selector, authority, lineage, cut, parent and claim of the family;
  - holder_attestation1 and command_grant_successor, inlined below (they appear in no root);
  - the kept S1 entries (identity, command_enroll_* ...), read from goldens.json, which this script
    never changes.
Every pre-twin entry is rebuilt from these sources first and checked against its inlined sha256
(the pre-twin goldens.json value), so the twins are the pre-twin family under one rule:

  Renumbering (parent lineage only): an AdmissionPermission is inserted at index 4, so every Log2
  coordinate with index >= 4 moves by +1. Lease scalars and HLCs are not coordinates. Derived
  digests are recomputed: GrantRef2 = sha256("VISROOT2-GRANT\\0" || OriginalGrant2), the claim digest
  = sha256("VISROOT2-CLAIMREF\\0" || claim), Accepted evidence = sha256("VISROOT2-EVIDENCE\\0" ||
  attestation), and command digests = sha256("VISROOT2-COMMAND\\0" || payload).

  Every root twin: control_len 241, control_digest = pin(M3_S1), the tip fingerprint kept at the
  placeholder f1.., command_digest per the twin table, Usage2.registry re-derived (68 selectors +
  121 authorities + 81 issuers + 117 permissions), checksum recomputed. The six parent-lineage roots
  hold the permission (authority 0x66.. rev 1, generation 0x11.., tenant 1, Raw, table 0, schema 0)
  enrolled at (1,1,4). The issuer roots and root_bootstrap_only are not renumbered.

Published TEST seeds (D-G3, public; they authorize nothing outside tests): M3_S1 admin 0xe1 (id
0x51), governance 0xe2 (id 0x52), recovery 0xe3 (id 0x53).

Layouts (little endian), as in statement_reference.py; in addition:
  Log2      = term u64 | node u32 | index u64, all zero for None
  envelope  = magic8 | version u16 | 1u16 | length u32 | body | sha256(prefix)
  root      = VISAUT04 v4: g | c | bootstrap | covered | fp | command | epoch u64 | control |
              control_len u64 | parents u32 | held u64 | history u64 | registry u64 | selectors |
              authorities | issuers | permissions (count u32, then u32 113 | key73 | enrolled |
              revoked) | lineages | cuts | parents   (each other section: count u32, u32-framed records)
  command   = VISEVT02 v2: g | c | kind u8 | fields;  result = VISRES02 v2: g | c | log | kind | fields
  wrapper   = VISMAS02 v2: 2 | genesis | bootstrap fp | bootstrap command | malformed u32 | len u32 | root
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


def sha(*parts):
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.digest()


# ---- Log2 is its own type, so renumbering never touches an HLC or a lease scalar ----
class Log(tuple):
    def shifted(self, k):
        return self if self[2] < 4 else Log((self[0], self[1], self[2] + k))


def log(i):
    return Log((1, 1, i))


class R:
    def __init__(s, b):
        s.b, s.p = b, 0

    def take(s, n):
        x = s.b[s.p:s.p + n]
        assert len(x) == n
        s.p += n
        return x

    def u8(s): return s.take(1)[0]
    def u16(s): return struct.unpack("<H", s.take(2))[0]
    def u32(s): return struct.unpack("<I", s.take(4))[0]
    def u64(s): return struct.unpack("<Q", s.take(8))[0]
    def i64(s): return struct.unpack("<q", s.take(8))[0]

    def log(s):
        b = s.take(20)
        return None if b == bytes(20) else Log(struct.unpack("<QIQ", b))

    def blob(s): return s.take(s.u32())

    def end(s):
        assert s.p == len(s.b), (s.p, len(s.b))


def wlog(x): return bytes(20) if x is None else struct.pack("<QIQ", *x)
def wblob(b): return struct.pack("<I", len(b)) + b


def whole(parse, b):
    r = R(b)
    v = parse(r)
    r.end()
    return v


# ---- records ----
def p_hlc(r): return (r.u64(), r.u32(), r.u32())
def e_hlc(h): return struct.pack("<QII", *h)
def p_auth(r): return (r.take(16), r.u64())
def e_auth(a): return a[0] + struct.pack("<Q", a[1])
def p_node(r): return (r.u32(), r.take(16))
def e_node(n): return struct.pack("<I", n[0]) + n[1]
def p_gref(r): return {"lineage": r.take(16), "log": r.log(), "digest": r.take(32)}
def e_gref(g): return g["lineage"] + wlog(g["log"]) + g["digest"]


def p_lease(r):
    return {"epoch": r.u64(), "holder": r.u32(), "rs": r.u64(), "re": r.u64(), "start": p_hlc(r),
            "expiry": p_hlc(r), "mci": r.u64(), "issuer": r.u32(), "sig": r.take(64)}


def e_lease(x):
    return (struct.pack("<QIQQ", x["epoch"], x["holder"], x["rs"], x["re"]) + e_hlc(x["start"])
            + e_hlc(x["expiry"]) + struct.pack("<QI", x["mci"], x["issuer"]) + x["sig"])


def p_pred(r):
    flag, b = r.u8(), r.take(68)
    if flag == 0:
        assert b == bytes(68)
        return None
    assert flag == 1
    return whole(p_gref, b)


def e_pred(p): return b"\x00" + bytes(68) if p is None else b"\x01" + e_gref(p)


def p_orig(r):
    return {"lineage": r.take(16), "inc": r.take(16), "lease": p_lease(r), "log": r.log(), "pred": p_pred(r)}


def e_orig(o): return o["lineage"] + o["inc"] + e_lease(o["lease"]) + wlog(o["log"]) + e_pred(o["pred"])
def grant_ref(o): return {"lineage": o["lineage"], "log": o["log"], "digest": sha(b"VISROOT2-GRANT\x00", e_orig(o))}
def p_lineage(r): return {"latest": p_orig(r), "tag": r.u8(), "clog": r.log(), "succ": r.take(16)}
def e_lineage(g): return e_orig(g["latest"]) + bytes([g["tag"]]) + wlog(g["clog"]) + g["succ"]


def p_claim(r):
    assert r.u16() == 2
    c = {"manifest": r.blob(), "mc": r.take(32), "orig": p_orig(r), "placement": r.take(32)}
    assert r.u8() == 1
    c["auth"] = p_auth(r)
    c["stamps"] = r.take(24 * r.u16())  # rows of HLC stamps, opaque here
    c["plans"] = [r.blob() for _ in range(r.u16())]
    c["delta"], c["cc"], c["sig"] = r.i64(), r.take(32), r.take(64)
    return c


def e_claim(c):
    return (struct.pack("<H", 2) + wblob(c["manifest"]) + c["mc"] + e_orig(c["orig"]) + c["placement"] + b"\x01"
            + e_auth(c["auth"]) + struct.pack("<H", len(c["stamps"]) // 24) + c["stamps"]
            + struct.pack("<H", len(c["plans"])) + b"".join(wblob(p) for p in c["plans"])
            + struct.pack("<q", c["delta"]) + c["cc"] + c["sig"])


def claim_digest(c): return sha(b"VISROOT2-CLAIMREF\x00", e_claim(c))


def p_att(r):
    assert r.u16() == 1
    a = {"grant": p_gref(r), "res": r.log(), "cd": r.take(32), "auth": p_auth(r), "children": []}
    for _ in range(r.u16()):
        child, db, k = r.u32(), r.take(32), r.u8()
        a["children"].append((child, db, [p_node(r) for _ in range(k)]))
    a["sig"] = r.take(64)
    return a


def e_att(a):
    b = struct.pack("<H", 1) + e_gref(a["grant"]) + wlog(a["res"]) + a["cd"] + e_auth(a["auth"])
    b += struct.pack("<H", len(a["children"]))
    for child, db, nodes in a["children"]:
        b += struct.pack("<I", child) + db + bytes([len(nodes)]) + b"".join(e_node(n) for n in nodes)
    return b + a["sig"]


def evidence(a): return sha(b"VISROOT2-EVIDENCE\x00", e_att(a))


def p_parent(r):
    return {"claim": whole(p_claim, r.blob()), "res": r.log(), "held": r.u64(), "tag": r.u8(), "first": r.log(),
            "decision": r.u8(), "cause": r.u8(), "anchor": r.log(), "evidence": r.take(32)}


def e_parent(p):
    return (wblob(e_claim(p["claim"])) + wlog(p["res"]) + struct.pack("<Q", p["held"]) + bytes([p["tag"]])
            + wlog(p["first"]) + bytes([p["decision"], p["cause"]]) + wlog(p["anchor"]) + p["evidence"])


def p_selector(r): return {"binding": r.take(44), "log": r.log()}
def e_selector(s): return s["binding"] + wlog(s["log"])


def p_authreg(r):
    return {"auth": p_auth(r), "node": p_node(r), "key": r.take(32), "log": r.log(), "flag": r.u8(), "rev": r.log()}


def e_authreg(a): return e_auth(a["auth"]) + e_node(a["node"]) + a["key"] + wlog(a["log"]) + bytes([a["flag"]]) + wlog(a["rev"])
def p_issuer(r): return {"node": r.u32(), "key": r.take(32), "log": r.log(), "flag": r.u8(), "rev": r.log()}
def e_issuer(i): return struct.pack("<I", i["node"]) + i["key"] + wlog(i["log"]) + bytes([i["flag"]]) + wlog(i["rev"])
def p_cut(r): return {"prior": r.u64(), "new": r.u64(), "log": r.log(), "config": r.take(32)}
def e_cut(c): return struct.pack("<QQ", c["prior"], c["new"]) + wlog(c["log"]) + c["config"]
def p_perm(r): return {"key": r.take(73), "enrolled": r.log(), "revoked": r.log()}
def e_perm(p): return p["key"] + wlog(p["enrolled"]) + wlog(p["revoked"])


# ---- envelopes ----
def envelope(magic, version, body):
    b = magic + struct.pack("<HHI", version, 1, len(body)) + body
    return b + sha(b)


def opened(b, magic, version):
    assert b[:8] == magic and struct.unpack("<HH", b[8:12]) == (version, 1)
    n = struct.unpack("<I", b[12:16])[0]
    assert n + 48 == len(b) and sha(b[:-32]) == b[-32:]
    return b[16:16 + n]


def p_command(b):
    r = R(opened(b, b"VISEVT02", 2))
    c = {"g": r.take(16), "c": r.take(16), "kind": r.u8()}
    k = c["kind"]
    if k == 0:
        c.update(epoch=r.u64(), control=r.take(32), length=r.u64())
    elif k == 1:
        c.update(ckind=r.u8(), external=r.take(32))
    elif k == 5:
        c.update(lineage=r.take(16), inc=r.take(16), lease=p_lease(r), pred=p_pred(r))
    elif k == 6:
        c.update(lineage=r.take(16), latest=p_gref(r))
    elif k == 7:
        c.update(expected=r.u64(), next=r.u64(), config=r.take(32))
    elif k == 8:
        c.update(claim=whole(p_claim, r.blob()))
    elif k == 9:
        c.update(identity=r.take(73), digest=r.take(32), res=r.log(), att=whole(p_att, r.blob()))
    elif k == 10:
        c.update(identity=r.take(73), digest=r.take(32), res=r.log(), cause=r.u8(), anchor=r.log())
    elif k == 11:
        c.update(statement=r.take(337))
    else:
        raise ValueError(k)
    r.end()
    return c


def command_payload(c):
    k = c["kind"]
    b = c["g"] + c["c"] + bytes([k])
    if k == 0:
        b += struct.pack("<Q", c["epoch"]) + c["control"] + struct.pack("<Q", c["length"])
    elif k == 1:
        b += bytes([c["ckind"]]) + c["external"]
    elif k == 5:
        b += c["lineage"] + c["inc"] + e_lease(c["lease"]) + e_pred(c["pred"])
    elif k == 6:
        b += c["lineage"] + e_gref(c["latest"])
    elif k == 7:
        b += struct.pack("<QQ", c["expected"], c["next"]) + c["config"]
    elif k == 8:
        b += wblob(e_claim(c["claim"]))
    elif k == 9:
        b += c["identity"] + c["digest"] + wlog(c["res"]) + wblob(e_att(c["att"]))
    elif k == 10:
        b += c["identity"] + c["digest"] + wlog(c["res"]) + bytes([c["cause"]]) + wlog(c["anchor"])
    elif k == 11:
        b += c["statement"]
    else:
        raise ValueError(k)
    return b


def e_command(c): return envelope(b"VISEVT02", 2, command_payload(c))
def command_digest(c): return sha(b"VISROOT2-COMMAND\x00", command_payload(c))


def e_result(x):
    k = x["kind"]
    b = G + C + wlog(x["log"]) + bytes([k])
    if k in (0, 1):
        pass
    elif k == 2:
        b += wlog(x["reg"])
    elif k == 3:
        b += wblob(e_lineage(x["lineage"]))
    elif k == 4:
        b += x["lid"] + bytes([x["cause"]]) + wlog(x["first"])
    elif k == 5:
        b += wblob(e_parent(x["parent"])) + bytes([x["flag"]]) + wlog(x["nfirst"]) + struct.pack("<q", x["delta"])
    elif k == 6:
        b += struct.pack("<H", x["reason"]) + wlog(x["ref"])
    elif k in (7, 8):
        b += bytes([x["n"]])
    elif k == 9:
        b += bytes([x["outcome"]]) + x["key"] + wlog(x["enrolled"]) + wlog(x["revoked"])
    else:
        raise ValueError(k)
    return envelope(b"VISRES02", 2, b)


def section(r, parse): return [whole(parse, r.blob()) for _ in range(r.u32())]
def w_section(xs, enc): return struct.pack("<I", len(xs)) + b"".join(wblob(enc(x)) for x in xs)


def p_root(b):
    r = R(opened(b, b"VISAUT04", 4))
    s = {"g": r.take(16), "c": r.take(16), "boot": r.log(), "covered": r.log(), "fp": r.take(32),
         "cmd": r.take(32), "epoch": r.u64(), "control": r.take(32), "clen": r.u64(), "parents_n": r.u32(),
         "held": r.u64(), "history": r.u64(), "registry": r.u64()}
    s["selectors"], s["authorities"], s["issuers"] = section(r, p_selector), section(r, p_authreg), section(r, p_issuer)
    s["permissions"] = []
    for _ in range(r.u32()):
        assert r.u32() == 113
        s["permissions"].append(p_perm(r))
    s["lineages"], s["cuts"], s["parents"] = section(r, p_lineage), section(r, p_cut), section(r, p_parent)
    r.end()
    return s


def e_root(s):
    b = (s["g"] + s["c"] + wlog(s["boot"]) + wlog(s["covered"]) + s["fp"] + s["cmd"] + struct.pack("<Q", s["epoch"])
         + s["control"] + struct.pack("<QIQQQ", s["clen"], s["parents_n"], s["held"], s["history"], s["registry"]))
    b += w_section(s["selectors"], e_selector) + w_section(s["authorities"], e_authreg)
    b += w_section(s["issuers"], e_issuer)
    b += struct.pack("<I", len(s["permissions"])) + b"".join(struct.pack("<I", 113) + e_perm(p) for p in s["permissions"])
    b += w_section(s["lineages"], e_lineage) + w_section(s["cuts"], e_cut) + w_section(s["parents"], e_parent)
    return envelope(b"VISAUT04", 4, b)


# ---- the renumbering rule, applied to any decoded structure ----
def renumbered(v, digests):
    """Every Log2 with index >= 4 moves by +1; every 32-byte value in `digests` is replaced."""
    if isinstance(v, Log):
        return v.shifted(1)
    if isinstance(v, dict):
        return {k: renumbered(x, digests) for k, x in v.items()}
    if isinstance(v, list):
        return [renumbered(x, digests) for x in v]
    if isinstance(v, tuple):
        return tuple(renumbered(x, digests) for x in v)
    if isinstance(v, bytes) and len(v) == 32 and v in digests:
        return digests[v]
    return v


# ---- Inputs inlined, so a rerun is unchanged once goldens.json holds the twins ----
G, C = bytes([0x11]) * 16, bytes([0x12]) * 16
GENESIS_S1 = bytes([0x11]) * 32  # genesis_s1: the fingerprint of the index-0 Membership entry
PLACEHOLDER_FP = bytes([0xF1]) * 32
# The pre-twin holder_attestation1 and command_grant_successor (goldens.json before the twins).
ATTESTATION = bytes.fromhex(
    "0100999999999999999999999999999999990100000000000000010000000400000000000000924f0804cc56"
    "b4a5919a6a87343140342efd8662cfa57c3fa3ccf6724856d6d9010000000000000001000000050000000000"
    "0000b53fdec061b11f97b287fea380f1d474ecd0670c05b2436f3c72edf9dc2d631466666666666666666666"
    "6666666666660100000000000000010000000000dddddddddddddddddddddddddddddddddddddddddddddddd"
    "dddddddddddddddd02010000007777777777777777777777777777777702000000bbbbbbbbbbbbbbbbbbbbbb"
    "bbbbbbbbbb000000000000000000000000000000000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000"
)
SUCCESSOR = bytes.fromhex(
    "5649534556543032020001000e01000011111111111111111111111111111111121212121212121212121212"
    "12121212059a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9abbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb01000000000000"
    "0002000000000000000000000063000000000000001e000000000000000000000001000000d0070000000000"
    "0000000000010000000100000000000000010000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000019999"
    "99999999999999999999999999990100000000000000010000000400000000000000924f0804cc56b4a5919a"
    "6a87343140342efd8662cfa57c3fa3ccf6724856d6d98a266ab7a23905e4f2add5613b1ee5eda01327ce9571"
    "ec42aefb08ddfdca37c0"
)
# sha256 of every pre-twin entry this script replaces (goldens.json before the twins).
OLD_SHA256 = {
    "grant_ref2": "f3c75ae68c580a32ac697c02d376f7fefa3fa43ad92afe130ba2bca656c422eb",
    "lineage_record_open": "397d2001116093a9892bcddf5d2ae257d0f11a054e15a5e8609142e50a3b55f5",
    "claim_projection2": "7bb4957ac98eb87a56b93725120bb1f6bc1cbc8917bb7514f3932725731808a9",
    "holder_attestation1": "5faa0022b0f78037005199638ab09a7c8e4a49b5b354bb48827319faa6ec35f0",
    "parent_reserved": "6c64c1f67c92106e24479c302dfdb1ef8e51f7bf0d846a81a657c22a7dadf5de",
    "parent_accepted": "4fa13df4b2dec6edc17e2fb1cb80f9d1641e3ed32c138693df680e1554d55caa",
    "parent_refused_cut": "e4e643d0a89095bd37a5d46a4e8b58c3f7a6d39ec9c18a6a2b06186fb77d38ff",
    "epoch_cut2": "faaddb738b3526c180c30bf794336a1aab5b2f260c571f733447c632bc73e494",
    "command_release_grant": "6fd694cacb94bba5762d764b680cfc7faba9cc57f7cdbe0938797d239563a21c",
    "command_reserve": "b1c2da2b414c2e6dde43c4f161243457ab9ce4fbea1e9a46c99a336340ff5982",
    "command_finalize": "5356cb4ac5e36ec083705ccef948cb406676ab260bc6feb5567ff931d7e9f8c9",
    "command_resolve_epoch_cut": "9cf6a3d5b7fb9be9a463ba8e61268af44a41ed42c5a78abc1b654eff890fb0f7",
    "command_grant_successor": "a369874f595bd46f7886f0392b805abb30209f0ecb32303854cc9c8b51976839",
    "root_bootstrap_only": "e401f2e77eeea1dbe6c3552ab45c2f3381ef61b89f5d0e8a5d1b2ea5a8e94bb9",
    "root_reserved": "ec89c11490755a6d06bb222e1898c177f72410ea29081259dd40f98a3dee63a0",
    "root_accepted": "d6a4d4651a3cdb9fd15ce83bf77dbb9b07ad3b23428f630cd70f52eb3102010b",
    "root_precut_accepted_preserved": "39e0fd344e076c0d5a81b656441b3b6943641fd88b1dc278511fb05ed4c61a24",
    "root_cut_first_refused": "9816985fc44e5fb3b5a268027c4c90df592a31cb133bc596b2e70e3d70c1cb4e",
    "result_covered": "762020762f7412c9221e7a709f9b1c6ff26385b507ccd5b2be5c8b93dddfc1b7",
    "result_already_processed": "543b209f0b53c17d80faabd6f110386708971cc0cef4e61e0ab5e351f634534e",
    "result_granted": "4328faa7c886cde25fba03f8f7a2dab3faf8d38b728131cdb99552acbf73b715",
    "result_closed": "7487dc32a9c572302563a2918df0f0bc8d42a0657d53fce0e2fb5f0845288182",
    "result_reserved": "2107d52cf8a60ec9b49a2ca7b92eb1bd1bb55b234d213d1836a0995042e87e1d",
    "result_new_accepted": "89b997071d0d344194156d54e3eeef0202ecb7a3910f7c8efc173c399028d180",
    "result_replayed_accepted": "26b675c384dea1593717c62a4a7b8571a2ee6be1babc474048e3b4a3e56d4572",
    "result_new_refused": "93687253bc9701ae6f6220c8358ea6609bbc4bb5c667651ba2c9492e3262fa58",
    "result_denied_capacity": "16002e3d4383be7a16321447c14126f0e288fb7d924f1339a902f0044ac58750",
    "result_pending_evidence": "c4def694973e502d36ae8b4c1f019bb8bcad1449479cce131fcbc07cd60bb214",
    "result_rejected_evidence": "402a74e347f678bed4b3de77040a8dc4a28e4124d64bad8102808dfd7c30f15b",
    "original_grant2": "2e64400dd25a65f6ed8253cdb595041acfa86084a4ca607d405a8366eb30776c",
    "lineage_record_renewed": "040228cd71e888e5e0d5e8a6992b4ba67805f39f8e84c8e86d9e7ac3592f5e91",
    "root_renewed_no_parents": "5151aea1914b6c699e4c0e946e3df1eeee18b6f7193a1d393fd8806bf5abe3aa",
    "root_renew_then_reserve_old_interval": "47ad243823e073fc9a266fdfc14eba5d0568116377feb1e7ed9bd5e8f2f6176d",
    "result_reserved_under_old_interval_after_renewal": "400af37b77bbc5e27c869285e4810ede9472104e1f15eab257628dfd757e9183",
    "root_one_issuer": "a9045c766e9262f94e2d7792e2eb421ac19eb2d8c2d5a49adae01e58ee3284ab",
    "root_numeric_issuers": "be9dd4513cff4c2d4cde93bb202cc8a2c642bdd1da4ac9e96a9d2c70fe48cd57",
}


def merge(name, update, indent):
    path = HERE / name
    text = path.read_text()
    newline = text.endswith("\n")
    old = json.loads(text)
    assert (json.dumps(old, indent=indent) + ("\n" if newline else "")) == text, name
    (OUT / name).write_text(json.dumps(update(old), indent=indent) + ("\n" if newline else ""))


def unhex(s): return bytes.fromhex(s)


GOLDENS = json.loads((HERE / "goldens.json").read_text())
KEPT = {f["name"]: unhex(f["hex"]) for f in GOLDENS["fixtures"]}
CORPUS = json.loads((HERE / "removed_forms.json").read_text())
OLD_ROOTS = {e["part"]: unhex(e["hex"]) for e in CORPUS["entries"] if e["id"] == "N13" and e["part"].startswith("root_")}
assert len(OLD_ROOTS) == 9

# ---- M3_S1 (bytes as statement_reference.py and profile_three_fixture.rs have them) ----
def private(seed): return Ed25519PrivateKey.from_private_bytes(bytes([seed]) * 32)
def public(seed): return private(seed).public_key().public_bytes_raw()


M3_S1 = (b"VISADM2\x00" + struct.pack("<HH", 1, 3) + G + C + struct.pack("<Q", 1)
         + bytes([0x51]) * 16 + public(0xE1) + struct.pack("<I", 3)
         + struct.pack("<QB", 1, 0) + bytes(16) + struct.pack("<Q", 0)
         + bytes([0x52]) * 16 + public(0xE2) + struct.pack("<I", 0x1FF)
         + bytes([0x53]) * 16 + public(0xE3) + struct.pack("<I", 0x1F))
assert len(M3_S1) == 241
PIN = sha(b"VISROOT2-ADMIN-MANIFEST\x00", M3_S1)
assert PIN.hex() == "00defc774bf27492c007834c61f68c4ec0c21dc683c1490b4307ee7b6be3e4c5"

# ---- the pre-twin family, rebuilt from the sources and checked against OLD_SHA256 ----
ROOTS = {name: p_root(b) for name, b in OLD_ROOTS.items()}
for name, s in ROOTS.items():
    assert e_root(s) == OLD_ROOTS[name], name
    assert (s["g"], s["c"], s["boot"], s["fp"], s["clen"]) == (G, C, log(1), PLACEHOLDER_FP, 0), name
RESERVED, ACCEPTED = ROOTS["root_reserved"], ROOTS["root_accepted"]
PRECUT, CUT_FIRST = ROOTS["root_precut_accepted_preserved"], ROOTS["root_cut_first_refused"]
RENEWED, OLD_INTERVAL = ROOTS["root_renewed_no_parents"], ROOTS["root_renew_then_reserve_old_interval"]
(LINEAGE_OPEN,) = RESERVED["lineages"]
(LINEAGE_RENEWED,) = RENEWED["lineages"]
(PARENT_RESERVED,), (PARENT_ACCEPTED,) = RESERVED["parents"], ACCEPTED["parents"]
(PARENT_REFUSED,), (PARENT_OLD_INTERVAL,) = CUT_FIRST["parents"], OLD_INTERVAL["parents"]
(CUT,) = PRECUT["cuts"]
CLAIM = PARENT_RESERVED["claim"]
ORIGINAL = CLAIM["orig"]
assert ORIGINAL == LINEAGE_OPEN["latest"]
assert all(p["claim"] == CLAIM for p in (PARENT_ACCEPTED, PARENT_REFUSED, PARENT_OLD_INTERVAL))
IDENTITY = KEPT["identity"]
assert CLAIM["manifest"] == KEPT["manifest1"] and CLAIM["manifest"][2:75] == IDENTITY


def old_family():
    """The pre-twin entries, as functions of the decoded sources only."""
    att = whole(p_att, ATTESTATION)
    succ = p_command(SUCCESSOR)
    ref = grant_ref(ORIGINAL)
    assert att["grant"] == ref and att["cd"] == claim_digest(CLAIM) and succ["pred"] == ref
    assert att["res"] == PARENT_RESERVED["res"] and PARENT_ACCEPTED["evidence"] == evidence(att)
    cmd = lambda kind, **f: dict(g=G, c=C, kind=kind, **f)
    res = lambda at, kind, **f: dict(log=at, kind=kind, **f)
    claim_d = claim_digest(CLAIM)
    return {
        "grant_ref2": ("body", e_gref, ref),
        "lineage_record_open": ("body", e_lineage, LINEAGE_OPEN),
        "claim_projection2": ("body", e_claim, CLAIM),
        "holder_attestation1": ("body", e_att, att),
        "parent_reserved": ("body", e_parent, PARENT_RESERVED),
        "parent_accepted": ("body", e_parent, PARENT_ACCEPTED),
        "parent_refused_cut": ("body", e_parent, PARENT_REFUSED),
        "epoch_cut2": ("body", e_cut, CUT),
        "command_release_grant": ("command_envelope", e_command,
                                  cmd(6, lineage=ORIGINAL["lineage"], latest=ref)),
        "command_reserve": ("command_envelope", e_command, cmd(8, claim=CLAIM)),
        "command_finalize": ("command_envelope", e_command,
                             cmd(9, identity=IDENTITY, digest=claim_d, res=PARENT_RESERVED["res"], att=att)),
        "command_resolve_epoch_cut": ("command_envelope", e_command,
                                      cmd(10, identity=IDENTITY, digest=claim_d, res=PARENT_REFUSED["res"],
                                          cause=PARENT_REFUSED["cause"], anchor=PARENT_REFUSED["anchor"])),
        "command_grant_successor": ("command_envelope", e_command, succ),
        "result_covered": ("untrusted_result_envelope", e_result, res(log(9), 0)),
        "result_already_processed": ("untrusted_result_envelope", e_result, res(log(7), 1)),
        "result_granted": ("untrusted_result_envelope", e_result, res(ORIGINAL["log"], 3, lineage=LINEAGE_OPEN)),
        "result_closed": ("untrusted_result_envelope", e_result,
                          res(log(8), 4, lid=ORIGINAL["lineage"], cause=3, first=CUT["log"])),
        "result_reserved": ("untrusted_result_envelope", e_result,
                            res(PARENT_RESERVED["res"], 5, parent=PARENT_RESERVED, flag=0, nfirst=None, delta=0)),
        "result_new_accepted": ("untrusted_result_envelope", e_result,
                                res(PARENT_ACCEPTED["first"], 5, parent=PARENT_ACCEPTED, flag=1,
                                    nfirst=PARENT_ACCEPTED["first"], delta=CLAIM["delta"])),
        "result_replayed_accepted": ("untrusted_result_envelope", e_result,
                                     res(log(9), 5, parent=PARENT_ACCEPTED, flag=0, nfirst=None, delta=0)),
        "result_new_refused": ("untrusted_result_envelope", e_result,
                               res(PARENT_REFUSED["first"], 5, parent=PARENT_REFUSED, flag=0, nfirst=None, delta=0)),
        "result_denied_capacity": ("untrusted_result_envelope", e_result, res(log(9), 6, reason=10, ref=None)),
        "result_pending_evidence": ("untrusted_result_envelope", e_result, res(log(6), 7, n=1)),
        "result_rejected_evidence": ("untrusted_result_envelope", e_result, res(log(6), 8, n=2)),
        "original_grant2": ("body", e_orig, ORIGINAL),
        "lineage_record_renewed": ("body", e_lineage, LINEAGE_RENEWED),
        "result_reserved_under_old_interval_after_renewal": (
            "untrusted_result_envelope", e_result,
            res(PARENT_OLD_INTERVAL["res"], 5, parent=PARENT_OLD_INTERVAL, flag=0, nfirst=None, delta=0)),
    }


OLD = old_family()
OLD_BYTES = {name: enc(v) for name, (_, enc, v) in OLD.items()}
OLD_BYTES.update(OLD_ROOTS)
assert set(OLD_BYTES) == set(OLD_SHA256), set(OLD_BYTES) ^ set(OLD_SHA256)
for name, b in OLD_BYTES.items():
    assert sha(b).hex() == OLD_SHA256[name], name

# ---- the twins ----
old_att = OLD["holder_attestation1"][2]
new_original = renumbered(ORIGINAL, {})
new_claim = renumbered(CLAIM, {})
DIGESTS = {grant_ref(ORIGINAL)["digest"]: grant_ref(new_original)["digest"],
           claim_digest(CLAIM): claim_digest(new_claim)}
new_att = renumbered(old_att, DIGESTS)
DIGESTS[evidence(old_att)] = evidence(new_att)
assert new_att["grant"] == grant_ref(new_original) and new_att["cd"] == claim_digest(new_claim)

NEW = {}
for name, (kind, enc, v) in OLD.items():
    NEW[name] = (kind, enc(renumbered(v, DIGESTS)))

BOOTSTRAP = dict(g=G, c=C, kind=0, epoch=1, control=PIN, length=241)
FP_B = sha(b"TEST-S1-ENTRY\x00", command_payload(BOOTSTRAP))
BOOT_DIGEST = command_digest(BOOTSTRAP)
PERMISSION_KEY = (bytes([0x66]) * 16 + struct.pack("<Q", 1) + G + struct.pack("<QB", 1, 0) + bytes(16)
                  + struct.pack("<Q", 0))
assert RESERVED["authorities"][0]["auth"] == (PERMISSION_KEY[:16], 1) and CLAIM["auth"] == RESERVED["authorities"][0]["auth"]
CONTEXT = G + C + wlog(log(1)) + FP_B + BOOT_DIGEST + GENESIS_S1 + struct.pack("<Q", 1) + PIN
assert len(CONTEXT) == 188
SIGNED = b"VISADS02" + struct.pack("<HBB", 1, 1, 0) + CONTEXT + PERMISSION_KEY
STATEMENT = SIGNED + private(0xE1).sign(b"VISROOT2-ADMIN-STATEMENT\x00" + SIGNED)
assert len(STATEMENT) == 337
NEW["command_bootstrap_p3"] = ("command_envelope", e_command(BOOTSTRAP))
NEW["command_admission_permission_p3"] = ("command_envelope", e_command(dict(g=G, c=C, kind=11, statement=STATEMENT)))
NEW["result_permission_p3"] = ("untrusted_result_envelope", e_result(
    dict(log=log(4), kind=9, outcome=1, key=PERMISSION_KEY, enrolled=log(4), revoked=None)))

old_command = lambda name: command_digest(p_command(OLD_BYTES[name]))
new_command = lambda name: command_digest(p_command(NEW[name][1] if name in NEW else KEPT[name]))
# Twin table: the command golden each old root's command_digest is the digest of, and the twin's.
TWIN_COMMAND = {
    "root_bootstrap_only": ("command_bootstrap", "command_bootstrap_p3"),
    "root_one_issuer": ("command_bootstrap", "command_bootstrap_p3"),
    "root_numeric_issuers": ("command_bootstrap", "command_bootstrap_p3"),
    "root_reserved": ("command_reserve", "command_reserve"),
    "root_accepted": ("command_finalize", "command_finalize"),
    "root_precut_accepted_preserved": ("command_advance_epoch", "command_advance_epoch"),
    "root_cut_first_refused": ("command_resolve_epoch_cut", "command_resolve_epoch_cut"),
    "root_renewed_no_parents": ("command_grant_renewal", "command_grant_renewal"),
    "root_renew_then_reserve_old_interval": ("command_reserve", "command_reserve"),
}
PARENT_LINEAGE = [n for n in TWIN_COMMAND if not n.startswith(("root_bootstrap", "root_one", "root_numeric"))]
for name, s in ROOTS.items():
    old, new = TWIN_COMMAND[name]
    assert s["cmd"] == (old_command(old) if old in OLD_BYTES else command_digest(p_command(KEPT[old]))), name
    lineage = name in PARENT_LINEAGE
    t = renumbered(s, DIGESTS) if lineage else dict(s)
    t.update(cmd=new_command(new), control=PIN, clen=241)
    if lineage:
        assert not t["permissions"] and t["authorities"][0]["log"] == log(3)
        t["permissions"] = [{"key": PERMISSION_KEY, "enrolled": log(4), "revoked": None}]
    t["registry"] = (68 * len(t["selectors"]) + 121 * len(t["authorities"]) + 81 * len(t["issuers"])
                     + 117 * len(t["permissions"]))
    assert t["registry"] == s["registry"] + (117 if lineage else 0)
    NEW[name] = ("untrusted_root_envelope", e_root(t))

# ---- provenance: fixture -> history step. T-FAMILY replays every history from genesis ----
REGISTER = ["command_bootstrap_p3", "command_enroll_selector", "command_enroll_authority",
            "command_admission_permission_p3", "command_grant_initial"]
HISTORIES = {
    "bootstrap_only": {"from": None, "steps": ["command_bootstrap_p3"], "root": "root_bootstrap_only"},
    "reserved": {"from": None, "steps": REGISTER + ["command_reserve"], "root": "root_reserved"},
    "accepted": {"from": "reserved", "steps": ["command_finalize"], "root": "root_accepted"},
    "precut": {"from": "accepted", "steps": ["command_advance_epoch"], "root": "root_precut_accepted_preserved"},
    "cut_first": {"from": "reserved", "steps": ["command_cover_blank", "command_advance_epoch",
                                                "command_resolve_epoch_cut"], "root": "root_cut_first_refused"},
    "renewed": {"from": None, "steps": REGISTER + ["command_grant_renewal"], "root": "root_renewed_no_parents"},
    "renew_then_reserve": {"from": "renewed", "steps": ["command_reserve"],
                           "root": "root_renew_then_reserve_old_interval"},
    # Branches that reach the remaining results; they end at no root golden.
    "covered": {"from": "cut_first", "steps": ["command_cover_blank"], "root": None},
    "already_processed": {"from": "precut", "steps": ["replay_tip"], "root": None},
    "closed": {"from": "precut", "steps": ["command_grant_initial"], "root": None},
    "replayed_accepted": {"from": "closed", "steps": ["command_finalize"], "root": None},
    "pending_evidence": {"from": "reserved", "steps": ["finalize_one_ack"], "root": None},
    "rejected_evidence": {"from": "reserved", "steps": ["finalize_foreign_claim_digest"], "root": None},
    "denied_capacity": {"from": "renewed", "steps": ["command_cover_blank", "command_cover_blank",
                                                     "command_cover_blank", "reserve_without_slots"], "root": None},
}


def length(h):
    x = HISTORIES[h]
    # A replay of the tip entry processes the tip again; it does not advance the index.
    return (length(x["from"]) if x["from"] else 0) + sum(step != "replay_tip" for step in x["steps"])


RESULTS = {
    "result_registered": ("reserved", 3), "result_permission_p3": ("reserved", 4),
    "result_granted": ("reserved", 5), "result_reserved": ("reserved", 6),
    "result_new_accepted": ("accepted", 7), "result_new_refused": ("cut_first", 9),
    "result_reserved_under_old_interval_after_renewal": ("renew_then_reserve", 7),
    "result_covered": ("covered", 10), "result_already_processed": ("already_processed", 8),
    "result_closed": ("closed", 9), "result_replayed_accepted": ("replayed_accepted", 10),
    "result_pending_evidence": ("pending_evidence", 7), "result_rejected_evidence": ("rejected_evidence", 7),
    "result_denied_capacity": ("denied_capacity", 10),
}
for name, (h, i) in RESULTS.items():
    b = NEW[name][1] if name in NEW else KEPT[name]
    processed = struct.unpack("<QIQ", opened(b, b"VISRES02", 2)[32:52])
    assert processed == (1, 1, i) and (i == length(h) or name in ("result_registered", "result_permission_p3",
                                                                   "result_granted", "result_reserved")), name
for h, x in HISTORIES.items():
    if x["root"]:
        covered = p_root(NEW[x["root"]][1])["covered"]
        assert covered == log(length(h)), h
# Bodies and commands: the step whose command, record or state they are.
BODIES = {
    "grant_ref2": ("reserved", 5), "original_grant2": ("reserved", 5), "lineage_record_open": ("reserved", 5),
    "claim_projection2": ("reserved", 6), "command_reserve": ("reserved", 6), "parent_reserved": ("reserved", 6),
    "holder_attestation1": ("accepted", 7), "command_finalize": ("accepted", 7), "parent_accepted": ("accepted", 7),
    "epoch_cut2": ("precut", 8), "command_resolve_epoch_cut": ("cut_first", 9), "parent_refused_cut": ("cut_first", 9),
    "lineage_record_renewed": ("renewed", 6),
    "command_release_grant": ("released", 7), "command_grant_successor": ("superseded", 7),
    "command_bootstrap_p3": ("reserved", 1), "command_admission_permission_p3": ("reserved", 4),
}
# The release and the successor grant are applied by the S1 tests on root_reserved (index 7).
HISTORIES["released"] = {"from": "reserved", "steps": ["command_release_grant"], "root": None}
HISTORIES["superseded"] = {"from": "reserved", "steps": ["command_grant_successor"], "root": None}
PROVENANCE = {
    "genesis_s1": GENESIS_S1.hex(),
    "rule": "parent lineage only: AdmissionPermission at 4, every Log2 index >= 4 moved by +1",
    "histories": HISTORIES,
    "fixtures": {n: list(v) for n, v in sorted({**RESULTS, **BODIES}.items())},
}
for n, v in {**RESULTS, **BODIES}.items():
    assert n in NEW or n == "result_registered", n
assert set(NEW) - set(PROVENANCE["fixtures"]) == set(TWIN_COMMAND), set(NEW) - set(PROVENANCE["fixtures"])


def update_goldens(old):
    fixtures = []
    for f in old["fixtures"]:
        name = f["name"]
        if name in NEW:
            assert NEW[name][0] == f["kind"], name
            b = NEW[name][1]
            f = {"name": name, "kind": f["kind"], "length": len(b), "hex": b.hex(), "sha256": sha(b).hex()}
        else:
            b = unhex(f["hex"])
            assert f["length"] == len(b) and f["sha256"] == sha(b).hex(), name
        fixtures.append(f)
    for name in ("command_bootstrap_p3", "command_admission_permission_p3", "result_permission_p3"):
        if all(f["name"] != name for f in fixtures):
            kind, b = NEW[name]
            fixtures.append({"name": name, "kind": kind, "length": len(b), "hex": b.hex(), "sha256": sha(b).hex()})
    assert len(fixtures) == 58 and len({f["name"] for f in fixtures}) == 58
    return {**{k: v for k, v in old.items() if k not in ("fixtures", "provenance")}, "fixtures": fixtures,
            "provenance": PROVENANCE}


merge("goldens.json", update_goldens, 2)

# ---- origin_vectors.json p3_ keys: the wrapper holding the root_bootstrap_only twin ----
nested = NEW["root_bootstrap_only"][1]
WRAPPER = (bytes([2]) + GENESIS_S1 + PLACEHOLDER_FP + BOOT_DIGEST + struct.pack("<I", 0)
           + struct.pack("<I", len(nested)) + nested)


def update_origin(old):
    kept = {k: v for k, v in old.items() if not k.startswith("p3_")}
    kept.update({"p3_root": envelope(b"VISMAS02", 2, WRAPPER).hex(), "p3_root_fingerprint": PLACEHOLDER_FP.hex(),
                 "p3_root_command": BOOT_DIGEST.hex(), "p3_nested_root_len": len(nested)})
    return kept


merge("origin_vectors.json", update_origin, 1)
