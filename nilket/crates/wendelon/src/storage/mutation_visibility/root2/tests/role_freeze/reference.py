"""Independent synthetic codec/domain vectors; no committed authority or runtime claim."""
from pathlib import Path
import hashlib,json,struct
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
p=Path(__file__).parent
old=json.loads((p.parent/'role_epochs/vectors.json').read_text())
context=bytes.fromhex(old['context']);assert len(context)==188
context_digest=hashlib.sha256(b'VISROOT2-ROLE3-CONTEXT\0'+context).digest();assert len(context_digest)==32
u64=lambda n:struct.pack('<Q',n)
log=lambda n:struct.pack('<QIQ',1,1,n)
pub=lambda seed:Ed25519PrivateKey.from_private_bytes(bytes([seed])*32).public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
def envelope(body):
 b=b'VISAUT05'+struct.pack('<HBBI',5,1,0,len(body))+body
 return b+hashlib.sha256(b).digest()
base=bytearray(bytes.fromhex(old['state_v4'])[16:-32]);assert len(base)==240
roots=[]
for index,frozen in [(3,True),(4,False),(5,True)]:
 body=bytearray(base);body[52:72]=log(index)
 # Fixed body offset: section-count before held/history/registry and seven collection counts.
 body[196:204]=u64(120)
 body+=pub(11)+pub(12)+pub(13)+bytes(3)+bytes([int(frozen)])+log(index)
 assert len(body)==360
 roots.append(envelope(body).hex())
domains=[('freeze',[3,0],0,b'VISROOT2-ROLE3-FREEZE-RECOVERY\0'),('unfreeze',[1,3],0,b'VISROOT2-ROLE3-UNFREEZE-ADMIN\0'),('unfreeze',[1,3],1,b'VISROOT2-ROLE3-UNFREEZE-RECOVERY-WITH-ADMIN\0'),('unfreeze',[2,3],0,b'VISROOT2-ROLE3-UNFREEZE-GOVERNANCE\0'),('unfreeze',[2,3],1,b'VISROOT2-ROLE3-UNFREEZE-RECOVERY-WITH-GOVERNANCE\0')]
proofs=[]
for form,roles,slot,domain in domains:
 b=bytearray(224);b[:8]=b'VISROL03';b[8:10]=struct.pack('<H',1);b[12:44]=context_digest;b[44:76]=bytes([0x42])*32
 for role,at in zip(roles,[76,150]):
  b[at]=role
  if role:b[at+9]=1
 assert len(b)==224 and len(log(1))==20
 pre=domain+b[:86]+b[150:160];assert len(pre)==len(domain)+96
 seed={1:11,2:12,3:13}[roles[slot]];at=[86,160][slot];b[at:at+64]=Ed25519PrivateKey.from_private_bytes(bytes([seed])*32).sign(pre)
 assert len(b)==224
 proofs.append(dict(form=form,roles=roles,slot=slot,proof=b.hex(),preimage=pre.hex()))
(p/'vectors.json').write_text(json.dumps(dict(qualification='synthetic codec/domain vectors only, not independent issuer authorization oracle',states=roots,proofs=proofs),indent=2)+'\n')
