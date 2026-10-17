"""Independent terminal event/snapshot encoder, consuming frozen pre-slice source vectors."""
import hashlib,json,struct
from pathlib import Path
p=Path(__file__).parent
v=json.loads((p.parent/'profile_three_vectors.json').read_text())
approve=bytes.fromhex(v['events'][5])
identity=approve[36:132]
observation=bytearray(approve[144:352]);observation[:8]=b'VISAOB2\0';observation[200:208]=bytes(8)
log=struct.pack('<QIQ',1,1,9)
full=bytes(observation)+identity[64:96]+log+bytes([9])*32+bytes([10])*32+bytes([11])*32
sha=lambda b:hashlib.sha256(b).digest()
terminal=log+bytes([9])*32+bytes([10])*32+bytes([11])*32+sha(b'VISROOT2-RETIREMENT-OBSERVATION3\0'+full)+struct.pack('<Q',3)
event=b'VISOEV02'+struct.pack('<HBBQ',1,4,0,3)+bytes([9])*16+identity+struct.pack('<H',156)+bytes(10)+terminal+bytes(336-156)
assert len(event)==480
event+=sha(event)
snapshot=b'VISINV02'+struct.pack('<HIQ',4,1,3)+identity+bytes([4,3])+bytes(6)+struct.pack('<Q',3)+terminal+bytes(52)
snapshot+=sha(snapshot)
(p/'terminal_vectors.json').write_text(json.dumps(dict(event=event.hex(),snapshot=snapshot.hex()),indent=2)+'\n')
