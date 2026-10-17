"""Independent synthetic V6 state bytes; not an issuer or committed-prefix oracle."""
from pathlib import Path
import hashlib,json,struct
p=Path(__file__).parent
old=json.loads((p.parent/'role_freeze/vectors.json').read_text())
legacy=json.loads((p.parent/'role_epochs/vectors.json').read_text())
log=lambda n:struct.pack('<QIQ',1,1,n)
def wrap(body):
 b=b'VISAUT06'+struct.pack('<HBBI',6,1,0,len(body))+body
 return (b+hashlib.sha256(b).digest()).hex()
rows=[]
for i,s in enumerate(old['states']):
 body=bytearray(bytes.fromhex(s)[16:-32]);assert len(body)==360
 at=6+i;body[52:72]=log(at);body[196:204]=struct.pack('<Q',204)
 body+=log(at)+bytes([0x53])*32+bytes([0x54])*32
 assert len(body)==444
 rows.append(wrap(body))
body=bytearray(bytes.fromhex(legacy['state_v4'])[16:-32]);assert len(body)==240
body[52:72]=log(3);body[196:204]=struct.pack('<Q',204)
original=bytes.fromhex(old['states'][0])[256:352];assert len(original)==96
body+=original+bytes(3)+bytes(21)+log(3)+bytes([0x53])*32+bytes([0x54])*32
assert len(body)==444
rows.append(wrap(body))
(p/'vectors.json').write_text(json.dumps(dict(qualification='synthetic V6 codec candidates, not independent issuer authority',states=rows),indent=2)+'\n')
