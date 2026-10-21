#!/usr/bin/env python3
"""Independent 512-byte framing, snapshots and repair-v2 fixtures; source bytes only, no Rust.

The mixed-profile `events`, `snapshots.three` and `repair` keys are removed at c1 (their `p3_`
twins remain), and `retire` and `snapshots` at c2; the rest is unchanged."""
import hashlib,json,struct
# Manifests "a" (profile 1), "b" (profile 2) and the first profile-3 manifest, inlined.
M=[bytes.fromhex(h) for h in [
 '56495341444d320001000100212121212121212121212121212121212e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e210100000000000012121212121212121212121212121212884b8857f4eaa1613c61504db34d4beaf346517a0e31de3cddd4d9b4201d9d0b03000000050000000000000000000000000000000000000000000000000000000000000000',
 '56495341444d320001000200424242424242424242424242424242424d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4201000000000000717171717171717171717171717171712152f8d19b791d24453242e15f2eab6cb7cffa7b6a5ed30097960e069881db120300000005000000000000000000000000000000000000000000000000000000000000000091919191919191919191919191919191c6d0c796bb4c3c923615f8dc85e6fc35149eb6b1317de523d3a5085584f5c109ff010000',
 '56495341444d320001000300a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c201000000000000000d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0dd04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737030000000500000000000000000000000000000000000000000000000000000000000000006c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c8320a51977d8c38ca8a4927c670df5821e449761945e15e9efb26a1509d230eaff0100007c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7f747b67bd3fe63c2a736739dfe40156d622347346e70f68f51c178a75ce55371f000000',
]]
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
# p3_: the profile-3 root alone, its events renumbered as seq 1-2, its snapshot and a repair over them.
p3_events=[event(1,1,identity(M[2]),M[2]),event(2,2,identity(M[2]),record(M[2],2))]
p3_three=b'VISINV02'+struct.pack('<HIQ',3,1,2)+identity(M[2])+bytes([2,3])+bytes(6)+struct.pack('<Q',2)+record(M[2],2);p3_three+=sha(p3_three)
p3_prefix=b''.join(p3_events);p3_boundary=len(p3_prefix);p3_original=p3_boundary+len(tail)
p3_context=sha(b'VISROOT2-REPAIR-CONTEXT-V2\0'+sha(p3_prefix)+struct.pack('<QQ',p3_boundary,p3_original)+sha(tail));p3_op=sha(b'VISROOT2-REPAIR-OP-V2\0'+p3_context)[:16]
p3_evidence=b'VISREP04'+struct.pack('<HH',1,2)+sha(p3_prefix)+struct.pack('<QQI',p3_boundary,p3_original,len(tail))+bytes(4)+sha(tail)+p3_op+struct.pack('<Q',3)+tail;p3_evidence+=sha(p3_evidence)
marker=b'VISOWN02'+struct.pack('<HHI',2,512,0);marker+=sha(marker)
print(json.dumps({'marker':marker.hex(),'generator':'profile_three_reference.py','manifests':[m.hex()for m in M],'p3_events':[e.hex()for e in p3_events],'p3_three':p3_three.hex(),'p3_repair':{'tail':tail.hex(),'name':p3_context.hex()+'.rep','context':p3_context.hex(),'bytes':p3_evidence.hex()}},indent=2))
