from pathlib import Path
import subprocess, json, time
root=Path('/home/me/work/.agent-bridge/adr0173-hosts')
with (root/'drive-e2e.stderr').open('w') as err, (root/'drive-e2e.jsonl').open('w') as log:
    p=subprocess.Popen(['/home/me/work/ket/target/debug/polariton','mcp'],cwd='/home/me/work/ket',stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True)
    def send(value):
        log.write(json.dumps({'sent':value})+'\n');log.flush()
        p.stdin.write(json.dumps(value)+'\n');p.stdin.flush()
    def receive(wanted):
        while True:
            line=p.stdout.readline()
            if not line: raise RuntimeError('MCP EOF before result')
            value=json.loads(line);log.write(json.dumps({'received':value})+'\n');log.flush()
            if value.get('id')==wanted:return value
    send({'jsonrpc':'2.0','id':1,'method':'initialize','params':{'protocolVersion':'2025-06-18','capabilities':{},'clientInfo':{'name':'ADR0173-drive-regression','version':'1'}}})
    assert receive(1)['result']['protocolVersion']=='2025-06-18'
    send({'jsonrpc':'2.0','method':'notifications/initialized'})
    send({'jsonrpc':'2.0','id':2,'method':'tools/call','params':{'name':'drive','arguments':{'source':'uat/treasury_rates.rn','execution':'by-command','commands':[{'run_source':{}},{'describe':{}},{'publish':{'directory':str(root/'drive-package-final')}}]}}})
    response=receive(2); result=response['result'];receipt=result['structuredContent']
    assert not result['isError'],response
    assert [c['outcome'] for c in receipt['commands']]==['applied']*3
    assert len(receipt['commands'][0]['figures'])==4
    assert len(receipt['commands'][1]['figures'])==4
    assert len(receipt['commands'][2]['publish']['figures'])==4
    assert json.loads(result['content'][1]['text'])==receipt
    (root/'drive-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    p.stdin.close();p.wait(timeout=10)
    assert p.returncode==0
    print('MCP drive: run_source/describe/publish applied; 4 figures; clean EOF exit')
