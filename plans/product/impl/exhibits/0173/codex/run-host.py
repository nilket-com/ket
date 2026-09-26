from pathlib import Path
import subprocess,json,time,tempfile,os,hashlib
root=Path('/home/me/work/.agent-bridge/adr0173-hosts')
label='capped-codex'
binary=Path('/home/me/work/ket/target/debug/polariton')
runtime=Path(tempfile.mkdtemp(prefix='polariton-0173-codex-capped-'))
env=dict(os.environ,XDG_RUNTIME_DIR=str(runtime),XDG_CONFIG_HOME=str(root/'gui-config'),DISPLAY=':0')
env.pop('WAYLAND_DISPLAY',None)
(root/(label+'-binary.json')).write_text(json.dumps({'path':str(binary),'sha256':hashlib.sha256(binary.read_bytes()).hexdigest()},indent=2)+'\n')
with (root/(label+'-window.log')).open('w') as out:
 window=subprocess.Popen([str(binary),'--agents','/home/me/work/ket/uat/treasury_rates.rn'],cwd='/home/me/work/ket',env=env,stdout=out,stderr=subprocess.STDOUT)
try:
 deadline=time.monotonic()+45
 while True:
  files=list((runtime/'polariton').glob('*.json'))
  advertisements=[json.loads(p.read_text()) for p in files]
  found=[v for v in advertisements if v['pid']==window.pid]
  if found:break
  if window.poll() is not None:raise RuntimeError('fixture window exited')
  if time.monotonic()>deadline:raise TimeoutError('window discovery')
  time.sleep(.25)
 advertisement=found[0]
 (root/(label+'-window.json')).write_text(json.dumps(advertisement,indent=2)+'\n')
 args=json.loads((root/'final-codex-invocation.json').read_text())
 args=[a.replace('/tmp/polariton-0173-codex-yycvoy9r',str(runtime)) for a in args]
 prompt=(root/'final-codex-prompt.txt').read_text().replace('codex-final-package','capped-codex-package')
 prompt+='\nThe fixture window ID is '+advertisement['window']+'. Confirm it via windows and pin it explicitly.\n'
 config=json.loads((root/'final-codex-mcp-config.json').read_text())
 def replace(v):
  if isinstance(v,dict):return {k:replace(x) for k,x in v.items()}
  if isinstance(v,list):return [replace(x) for x in v]
  if isinstance(v,str):return v.replace('/tmp/polariton-0173-codex-yycvoy9r',str(runtime))
  return v
 (root/(label+'-mcp-config.json')).write_text(json.dumps(replace(config),indent=2)+'\n')
 (root/(label+'-invocation.json')).write_text(json.dumps(args,indent=2)+'\n')
 (root/(label+'-prompt.txt')).write_text(prompt)
 (root/(label+'-version.txt')).write_bytes(subprocess.check_output(['codex','--version']))
 with (root/(label+'-transcript.jsonl')).open('w') as out, (root/(label+'-stderr.log')).open('w') as err:
  result=subprocess.run(args,input=prompt,text=True,stdout=out,stderr=err,timeout=300)
 (root/(label+'-exit.txt')).write_text(str(result.returncode)+'\n')
 print('Codex host exit:',result.returncode,flush=True)
finally:
 window.terminate()
 try:window.wait(timeout=10)
 except subprocess.TimeoutExpired:window.kill();window.wait()
 import shutil
 shutil.rmtree(runtime)
