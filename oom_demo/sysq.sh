#!/bin/bash
# sysq.sh "<SQL>" — run on the serverless warehouse, poll, print column names + up to 40 rows.
set -uo pipefail
P=feast-demo; WH=f63c4e4bd1dc08d7
python3 -c "import json,sys; open('/tmp/sq.json','w').write(json.dumps({'warehouse_id':sys.argv[1],'statement':sys.argv[2],'wait_timeout':'30s'}))" "$WH" "$1"
J=$(databricks api post /api/2.0/sql/statements -p $P --json @/tmp/sq.json 2>&1)
SID=$(echo "$J" | python3 -c "import sys,json;print(json.load(sys.stdin).get('statement_id',''))" 2>/dev/null)
while true; do
  ST=$(echo "$J" | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',{}).get('state',''))" 2>/dev/null)
  case "$ST" in SUCCEEDED|FAILED|CANCELED|CLOSED|"") break;; esac
  sleep 8
  J=$(databricks api get /api/2.0/sql/statements/"$SID" -p $P 2>&1)
done
echo "$J" | python3 -c "
import sys,json
d=json.load(sys.stdin); st=d.get('status',{})
if st.get('state')!='SUCCEEDED':
    print('  STATE:',st.get('state'),'| ERR:',json.dumps(st.get('error'))[:300] if st.get('error') else ''); sys.exit()
cols=[c['name'] for c in d.get('manifest',{}).get('schema',{}).get('columns',[])]
print('  cols:',cols)
for r in d.get('result',{}).get('data_array',[])[:40]:
    print('  ',[ (str(x)[:70] if x is not None else None) for x in r])
"
