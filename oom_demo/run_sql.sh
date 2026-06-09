#!/bin/bash
# run_sql.sh "<SQL>" — run on the serverless SQL warehouse, poll to terminal, print state + error.
set -uo pipefail
P=feast-demo; WH=f63c4e4bd1dc08d7
python3 -c "import json,sys; open('/tmp/stmt.json','w').write(json.dumps({'warehouse_id':sys.argv[1],'statement':sys.argv[2],'wait_timeout':'30s'}))" "$WH" "$1"
J=$(databricks api post /api/2.0/sql/statements -p $P --json @/tmp/stmt.json 2>&1)
SID=$(echo "$J" | python3 -c "import sys,json;print(json.load(sys.stdin).get('statement_id',''))" 2>/dev/null)
echo "statement_id=$SID  sql=$1"
while true; do
  ST=$(echo "$J" | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',{}).get('state',''))" 2>/dev/null)
  echo "[$(date +%H:%M:%S)] state=$ST"
  case "$ST" in SUCCEEDED|FAILED|CANCELED|CLOSED|"") break;; esac
  sleep 12
  J=$(databricks api get /api/2.0/sql/statements/"$SID" -p $P 2>&1)
done
echo "$J" | python3 -c "import sys,json
d=json.load(sys.stdin); st=d.get('status',{})
print('FINAL:',st.get('state'))
if st.get('error'): print('ERROR:',json.dumps(st['error'])[:2500])
r=d.get('result',{})
if r.get('data_array'): print('result:',r['data_array'][:3])"
