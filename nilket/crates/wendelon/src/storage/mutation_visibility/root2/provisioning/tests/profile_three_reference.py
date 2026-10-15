#!/usr/bin/env python3
"""Independent 512-byte framing, snapshots and repair-v2 fixtures; source bytes only, no Rust."""
import hashlib,json,pathlib,struct
H=pathlib.Path(__file__).resolve().parent
old=json.loads((H/'profile_two_inventory_vectors.json').read_text())
three=json.loads((H/'../../admin/tests/profile_three_vectors.json').read_text())['valid'][0]
M=[bytes.fromhex(old['manifests'][k]['bytes']) for k in ['a','b']]+[bytes.fromhex(three['bytes'])]
def sha(b):return hashlib.sha256(b).digest()
def pin(m):return sha(b'VISROOT2-ADMIN-MANIFEST\0'+m)
def identity(m):return m[12:44]+pin(m)+m[68:100]
def record(m,seq):
 b=b'VISADR2\0'+struct.pack('<HH',1,1)+m[12:44]+pin(m)+m[44:52]+struct.pack('<QIQ',7,3,40+m[12])+bytes([0xa0])*32+bytes([0xb0])*32+bytes([0xc0])*32+struct.pack('<Q',seq)
 assert len(b)==208
 return b
def event(op,seq,ident,p):
 b=b'VISOEV02'+struct.pack('<HBBQ',1,op,0,seq)+bytes([seq])*16+ident.ljust(96,b'\0')+struct.pack('<H',len(p))+bytes(10)+p.ljust(336,b'\0')
 assert len(b)==480
 return b+sha(b)
events=[]
for i,m in enumerate(M):events +=[event(1,2*i+1,identity(m),m),event(2,2*i+2,identity(m),record(m,2*i+2))]
def snapshot(ms,version,retired=False):
 seq=7 if retired else 2*len(ms)
 b=b'VISINV02'+struct.pack('<HIQ',version,len(ms),seq)
 for i,m in enumerate(ms):
  isret=retired and i==2
  b+=identity(m)+bytes([4 if isret else 2,(i+1) if version!=1 else 0])+bytes(6)+struct.pack('<Q',7 if isret else 2*i+2)+(bytes(208)if isret else record(m,2*i+2))
 return b+sha(b)
prefix=b''.join(events);tail=b'abcde';boundary=len(prefix);original=boundary+len(tail)
context=sha(b'VISROOT2-REPAIR-CONTEXT-V2\0'+sha(prefix)+struct.pack('<QQ',boundary,original)+sha(tail));op=sha(b'VISROOT2-REPAIR-OP-V2\0'+context)[:16]
evidence=b'VISREP04'+struct.pack('<HH',1,2)+sha(prefix)+struct.pack('<QQI',boundary,original,len(tail))+bytes(4)+sha(tail)+op+struct.pack('<Q',7)+tail;evidence+=sha(evidence)
marker=b'VISOWN02'+struct.pack('<HHI',2,512,0);marker+=sha(marker)
print(json.dumps({'marker':marker.hex(),'generator':'profile_three_reference.py','manifests':[m.hex()for m in M],'events':[e.hex()for e in events],'retire':event(4,7,identity(M[2]),b'').hex(),'snapshots':{'one':snapshot(M[:1],1).hex(),'two':snapshot(M[:2],2).hex(),'three':snapshot(M,3).hex(),'retired':snapshot(M,3,True).hex()},'repair':{'tail':tail.hex(),'name':context.hex()+'.rep','context':context.hex(),'bytes':evidence.hex()}},indent=2))
