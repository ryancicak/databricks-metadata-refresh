#!/bin/bash
# poll_step.sh <step-id>  — poll an EMR step to terminal state, then print log tails.
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . ./aws_credentials.txt; set +a
CLUSTER="j-YOUREMRCLUSTERID"; REGION="us-west-2"; BUCKET="your-databricks-rootbucket"
STEP="$1"
STATE=""
while true; do
  STATE=$(aws emr describe-step --cluster-id "$CLUSTER" --step-id "$STEP" --region "$REGION" --query 'Step.Status.State' --output text 2>&1)
  echo "[$(date +%H:%M:%S)] step $STEP : $STATE"
  case "$STATE" in
    COMPLETED|FAILED|CANCELLED|INTERRUPTED) break ;;
  esac
  sleep 15
done
echo "=== FINAL: $STATE ==="
BASE="s3://${BUCKET}/emr-logs/${CLUSTER}/steps/${STEP}"
for f in stdout stderr; do
  if aws s3 cp "${BASE}/${f}.gz" "/tmp/${STEP}.${f}.gz" --region "$REGION" >/dev/null 2>&1; then
    echo "----- ${f} (tail) -----"
    gunzip -c "/tmp/${STEP}.${f}.gz" | tail -n 70
  else
    echo "(no ${f}.gz yet)"
  fi
done
