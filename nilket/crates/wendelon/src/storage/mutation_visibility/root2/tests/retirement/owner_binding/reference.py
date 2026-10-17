"""Independent synthetic retirement wire vectors; no authority or issuer claim."""
from pathlib import Path
import json,struct,hashlib
p=Path(__file__).parent
sha=lambda b:hashlib.sha256(b).digest()
log=lambda n:struct.pack('<QIQ',1,1,n)
obs=b'VISAOB2\0'+struct.pack('<HH',1,1)+bytes([1])*16+bytes([2])*16+bytes([3])*32+struct.pack('<Q',4)+log(5)+bytes([6])*32+bytes([7])*32+bytes([8])*32+bytes(8)
assert len(obs)==208
full=obs+bytes([9])*32+log(10)+bytes([11])*32+bytes([12])*32+bytes([13])*32
digest=sha(b'VISROOT2-RETIREMENT-OBSERVATION3\0'+full)
terminal=log(10)+bytes([11])*32+bytes([12])*32+bytes([13])*32+digest+struct.pack('<Q',3)
artifact=b'VISTRM03'+struct.pack('<HH',1,3)+bytes([1])*16+bytes([2])*16+bytes([3])*32+bytes([9])*32+terminal+struct.pack('<Q',4)+bytes([14])*32
artifact+=sha(artifact)
states=[]
for row in json.loads((p.parent/'vectors.json').read_text())['states']:
 raw=bytes.fromhex(row);body=bytearray(raw[16:-32]);retired=body[-84:]
 body[52:72]=retired[:20];body[72:104]=retired[20:52];body[104:136]=retired[52:84]
 normalized=raw[:16]+body;normalized+=sha(normalized)
 states.append(dict(input=row,normalized=normalized.hex(),digest=sha(b'VISROOT2-TERMINAL-STATE3\0'+normalized).hex()))
(p/'vectors.json').write_text(json.dumps(dict(observation=full.hex(),observation_digest=digest.hex(),terminal=terminal.hex(),artifact=artifact.hex(),states=states),indent=2)+'\n')
