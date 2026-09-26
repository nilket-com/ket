#!/bin/bash
# Usage: run-claude-host.sh <label>. Opens a fresh fixture window from the current
# ket debug binary, runs the real Claude Code MCP host sequence, closes the window.
set -u
L=$1; H=~/work/.agent-bridge/adr0173-hosts; B=/home/me/work/ket/target/debug/polariton
cd /home/me/work/ket
sha256sum $B > $H/$L-binary-sha256.txt
XDG_CONFIG_HOME=$H/gui-config DISPLAY=:0 nohup $B --agents /home/me/work/ket/uat/treasury_rates.rn > $H/$L-window.log 2>&1 &
P=$!; echo $P > $H/$L-window.pid; W=""
for i in $(seq 1 30); do W=$(grep -l "\"pid\":$P" /run/user/1003/polariton/*.json 2>/dev/null | head -1); [ -n "$W" ] && break; sleep 1; done
W=$(basename "$W" .json); echo "window $W"
sed "s#e37c5f2b61f044cdae95b57d9963e04c#$W#; s#adr0173-hosts/claude-package#adr0173-hosts/$L-package#" $H/attempt2-claude-prompt.txt > $H/$L-prompt.txt
cp $H/claude-mcp-config.json $H/$L-mcp-config.json
claude --version > $H/$L-version.txt
cd $H/claude-run
date -Is > $H/$L-started-at.txt
claude -p --tools "" --strict-mcp-config --mcp-config $H/$L-mcp-config.json --allowedTools "mcp__polariton__*" --output-format stream-json --verbose < $H/$L-prompt.txt > $H/$L-transcript.jsonl 2> $H/$L-stderr.log
echo "exit=$?" > $H/$L-exit.txt; date -Is >> $H/$L-started-at.txt
kill -TERM $P; sleep 2; kill -0 $P 2>/dev/null && echo "window still alive" || echo "window closed"
python3 - "$H/$L-transcript.jsonl" "$W" <<'PY'
import json,sys
f,w=sys.argv[1],sys.argv[2]; calls=[]; errs=0
for l in open(f):
    d=json.loads(l)
    if d.get('type')=='system' and d.get('subtype')=='init':
        print('init tools',len(d['tools']),'non-mcp',[t for t in d['tools'] if not t.startswith('mcp__')],d.get('claude_code_version'),d.get('model'))
    for b in (d.get('message') or {}).get('content') or []:
        if isinstance(b,dict) and b.get('type')=='tool_use':
            calls.append(b['name'].split('__')[-1]); assert b['input'].get('window',w)==w, b
        if isinstance(b,dict) and b.get('type')=='tool_result' and b.get('is_error'): errs+=1
    if d.get('type')=='result': print('result',d.get('subtype'),'is_error',d.get('is_error'))
print('calls',calls,'tool errors',errs)
PY
ls $H/$L-package/manifest.json 2>/dev/null && python3 -c "import json,sys;print(len(json.load(open(sys.argv[1]))['figures']),'figures')" $H/$L-package/manifest.json
