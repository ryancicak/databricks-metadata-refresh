#!/bin/bash
# poll_run.sh <job_run_id> — poll a Databricks job run to terminal, print result + error/OOM.
set -uo pipefail
P=feast-demo
RUN="$1"
LC=""
while true; do
  LC=$(databricks jobs get-run "$RUN" -p $P -o json 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('state',{}).get('life_cycle_state',''))" 2>/dev/null)
  echo "[$(date +%H:%M:%S)] life_cycle=$LC"
  case "$LC" in TERMINATED|INTERNAL_ERROR|SKIPPED|"") break;; esac
  sleep 15
done
databricks jobs get-run "$RUN" -p $P -o json > /tmp/run_$RUN.json 2>&1
python3 - "$RUN" <<'PYEOF'
import json,sys
run=sys.argv[1]
d=json.load(open(f'/tmp/run_{run}.json'))
s=d.get('state',{})
print("result_state=",s.get('result_state'))
print("state_message=",(s.get('state_message') or '')[:400])
ts=d.get('tasks',[])
trid=ts[0].get('run_id') if ts else None
print("task_run_id=",trid)
open(f'/tmp/trid_{run}.txt','w').write(str(trid or ''))
PYEOF
TRID=$(cat /tmp/trid_$RUN.txt 2>/dev/null)
if [ -n "$TRID" ]; then
  echo "=== task run output ==="
  databricks jobs get-run-output "$TRID" -p $P -o json 2>/dev/null | python3 -c "import sys,json
d=json.load(sys.stdin)
no=d.get('notebook_output',{})
if no.get('result'): print('OUTPUT:',no['result'][:1500])
if d.get('error'): print('ERROR:',str(d['error'])[:1500])
if d.get('error_trace'): print('TRACE(tail):',str(d['error_trace'])[-2500:])"
fi
