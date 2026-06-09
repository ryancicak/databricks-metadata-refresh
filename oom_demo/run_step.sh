#!/bin/bash
# run_step.sh <mode> [extra args...]
# Stages oom_demo.py to S3 and submits it as an EMR spark-submit step.
# Prints the StepId. Poll with: aws emr describe-step --cluster-id $CLUSTER --step-id <id>
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./aws_credentials.txt; set +a

CLUSTER="j-YOUREMRCLUSTERID"
REGION="us-west-2"
BUCKET="your-databricks-rootbucket"

# Stage the latest job script
aws s3 cp oom_demo/oom_demo.py "s3://${BUCKET}/oom_demo/scripts/oom_demo.py" --region "$REGION" >/dev/null
echo "staged script -> s3://${BUCKET}/oom_demo/scripts/oom_demo.py" >&2

python3 oom_demo/build_steps.py "$@" > /tmp/oom_steps.json
STEP_ID=$(aws emr add-steps --cluster-id "$CLUSTER" --region "$REGION" \
  --steps file:///tmp/oom_steps.json --query 'StepIds[0]' --output text)
echo "$STEP_ID"
