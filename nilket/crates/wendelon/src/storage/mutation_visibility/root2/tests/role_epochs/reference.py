"""Independent wire vectors only; synthetic contexts are not provenance or an auth oracle."""
import hashlib,json,struct
from pathlib import Path
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
u64=lambda n:struct.pack('<Q',n)
log=lambda n:struct.pack('<QIQ',1,1,n)
def key(n):return Ed25519PrivateKey.from_private_bytes(bytes([n])*32)
def pub(n):return key(n).public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
ctx=bytes([1])*16+bytes([2])*16+log(2)+b'B'*32+b'A'*32+b'G'*32+u64(7)+b'P'*32
assert len(ctx)==188
proof=bytearray(b'VISROL03'+struct.pack('<H',1)+b'\0\0'+hashlib.sha256(b'VISROOT2-ROLE3-CONTEXT\0'+ctx).digest()+b'D'*32+bytes([1])+u64(0)+b'\1'+bytes(64)+bytes([2])+u64(1)+b'\1'+bytes(64))
assert len(proof)==224
common=proof[:86]+proof[150:160];pre=[]
for slot,seed,domain,start in [(0,11,b'VISROOT2-ROLE3-JOINT-ADMIN-GOVERNANCE-ADMIN\0',86),(1,12,b'VISROOT2-ROLE3-JOINT-ADMIN-GOVERNANCE-GOVERNANCE\0',160)]:
 p=domain+common;sig=key(seed).sign(p);key(seed).public_key().verify(sig,p);proof[start:start+64]=sig;pre.append(p.hex())
namespace=bytes([1])*16+u64(4)+b'\0'+bytes(16)+u64(0)
statement=bytearray(b'VISADS03'+struct.pack('<H',1)+b'\1\0'+ctx+bytes([3])*16+u64(1)+namespace+u64(1))
assert len(statement)==281
statement_pre=b'VISROOT2-ROLE3-ADMIN-STATEMENT\0'+b'\1'+statement
statement+=key(40).sign(statement_pre);assert len(statement)==345
def envelope(magic,version,body):
 b=magic+struct.pack('<HBBI',version,1,0,len(body))+body
 return b+hashlib.sha256(b).digest()
def root(history):
 n=3 if history else 2;charge=172 if history else 0
 b=bytes([1])*16+bytes([2])*16+log(2)+log(n)+b'F'*32+b'D'*32+u64(7)+b'P'*32+u64(241)+struct.pack('<IQQQ',0,0,charge,0)+bytes(7*4)
 assert len(b)==240
 if history:b+=pub(11)+pub(12)+pub(13)+bytes([1,0,0])+pub(40)+log(3)+bytes(21)
 return envelope(b'VISAUT05' if history else b'VISAUT04',5 if history else 4,b)
obj={'qualification':'synthetic bytes/math only, no committed or ready state','context':ctx.hex(),'proof':proof.hex(),'proof_preimages':pre,'public_keys':[pub(11).hex(),pub(12).hex()],'statement':statement.hex(),'statement_preimage':statement_pre.hex(),'statement_key':pub(40).hex(),'state_v4':root(False).hex(),'state_v5':root(True).hex()}
Path(__file__).with_name('vectors.json').write_text(json.dumps(obj,indent=2)+'\n')
