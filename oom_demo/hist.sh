#!/bin/bash
# hist.sh "<fully.qualified.table>" — DESCRIBE HISTORY, poll to terminal, print labeled rows.
set -uo pipefail
P=feast-demo; WH=f63c4e4bd1dc08d7
python3 -c "import json,sys; open('/tmp/h.json','w').write(json.dumps({'warehouse_id':sys.argv[1],'statement':'DESCRIBE HISTORY '+sys.argv[2],'wait_timeout':'30s'}))" "$WH" "$1"
J=$(databricks api post /api/2.0/sql/statements -p $P --json @/tmp/h.json 2>&1)
SID=$(echo "$J" | python3 -c "import sys,json;print(json.load(sys.stdin).get('statement_id',''))" 2>/dev/null)
echo "statement_id=$SID"
while true; do
  ST=$(echo "$J" | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',{}).get('state',''))" 2>/dev/null)
  echo "  state=$ST"
  case "$ST" in SUCCEEDED|FAILED|CANCELED|CLOSED|"") break;; esac
  sleep 8
  J=$(databricks api get /api/2.0/sql/statements/"$SID" -p $P 2>&1)
done
echo "$J" | python3 -c "
import sys,json
d=json.load(sys.stdin); st=d.get('status',{})
print('FINAL:',st.get('state'))
if st.get('error'): print('ERROR:',json.dumps(st['error'])[:800]); sys.exit()
cols=[c['name'] for c in d.get('manifest',{}).get('schema',{}).get('columns',[])]
rows=d.get('result',{}).get('data_array',[])
print('rows:',len(rows))
for r in rows:
    rec=dict(zip(cols,r))
    print('  v'+str(rec.get('version')),'|',rec.get('operation'),'| params=',str(rec.get('operationParameters'))[:140])
    om=rec.get('operationMetrics')
    if om: print('       metrics=',str(om)[:240])
"
