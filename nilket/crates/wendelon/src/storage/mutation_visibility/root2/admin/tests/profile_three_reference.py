#!/usr/bin/env python3
"""Independent profile3 bytes and expected precedence. TEST public keys only; no Rust/generator imports."""
import hashlib,json,pathlib,struct
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
HERE=pathlib.Path(__file__).resolve().parent
base=json.loads((HERE/'profile_two_vectors.json').read_text())['valid']
recid=bytes([0x7c])*16
reckey=Ed25519PrivateKey.from_private_bytes(bytes([0x7b])*32).public_key().public_bytes_raw()
def edit(b,at,v):o=bytearray(b);o[at:at+len(v)]=v;return bytes(o)
valid=[]
for x in base:
 b=edit(bytes.fromhex(x['bytes']),10,struct.pack('<H',3))+recid+reckey+struct.pack('<I',31)
 assert len(b)==241
 valid.append({'name':x['name'],'bytes':b.hex(),'pin':hashlib.sha256(b'VISROOT2-ADMIN-MANIFEST\0'+b).hexdigest()})
b=bytes.fromhex(valid[0]['bytes']);errors=[]
def add(name,x,error):errors.append({'name':name,'bytes':x.hex(),'error':error})
add('readbound',bytes(4097),'OverReadBound');add('length',b[:-1],'Length');add('magic',edit(b,0,b'X'),'Magic');add('format',edit(b,8,struct.pack('<H',2)),'UnsupportedFormat');add('profile',edit(b,10,struct.pack('<H',4)),'UnsupportedProfile');add('pairing',edit(b,10,struct.pack('<H',2)),'ProfileLength')
for name,off in [('generation',12),('cluster',28),('admin',52),('governance',137),('recovery',189)]:add('zero_'+name,edit(b,off,bytes(16)),'ZeroField')
for role,off in [('Admin',100),('Governance',185),('Recovery',237)]:add('mask_'+role,edit(b,off,struct.pack('<I',0)), 'Action:'+role)
add('namespace',edit(b,112,b'\x02'),'Namespace')
for pair,ai,ak,bi,bk in [('AdminGovernance',52,68,137,153),('AdminRecovery',52,68,189,205),('GovernanceRecovery',137,153,189,205)]:
 add('id_'+pair,edit(b,bi,b[ai:ai+16]),'Reuse:'+pair+':Id');add('key_'+pair,edit(b,bk,b[ak:ak+32]),'Reuse:'+pair+':Key')
negative=json.loads((HERE/'vectors.json').read_text())['negative_keys']
for n in negative:
 # Existing independent enrollability corpus stores the encoded public key in `key`.
 key=bytes.fromhex(n['key'])
 for role,off in [('Admin',68),('Governance',153),('Recovery',205)]:add('key_'+n['name']+'_'+role,edit(b,off,key),'Key:'+role)
# Multiple faults pin mask before reuse, pair order, id before key, reuse before enrollability.
add('mask_before_reuse',edit(edit(b,189,b[52:68]),237,struct.pack('<I',0)),'Action:Recovery')
add('pair_order',edit(edit(b,137,b[52:68]),189,b[52:68]),'Reuse:AdminGovernance:Id')
add('id_before_key',edit(edit(b,137,b[52:68]),153,b[68:100]),'Reuse:AdminGovernance:Id')
add('reuse_before_bad_key',edit(edit(b,189,b[52:68]),205,bytes(32)),'Reuse:AdminRecovery:Id')
print(json.dumps({'generator':'profile_three_reference.py','recovery_id':recid.hex(),'recovery_key':reckey.hex(),'valid':valid,'errors':errors},indent=2))
