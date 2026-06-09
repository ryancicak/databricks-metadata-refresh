#!/bin/bash
# unregister_loc.sh — run once:  !bash ~/Documents/dbx-workspace-and-emr-iceberg/oom_demo/unregister_loc.sh
# Undo the earlier register-resource so the oom_demo S3 path is IAM-accessible again
# (Lake Formation registering it is what now blocks the EMR role from writing there).
# Reads are unaffected (they go through the storage credential).
cd "$(dirname "$0")/.." || exit 1
set -a; . ./aws_credentials.txt; set +a
aws lakeformation deregister-resource \
  --resource-arn arn:aws:s3:::your-databricks-rootbucket/oom_demo \
  --region us-west-2 && echo "DEREGISTERED ok"
echo "=== LF-registered locations now (should NOT list oom_demo) ==="
aws lakeformation list-resources --region us-west-2 --query 'ResourceInfoList[].ResourceArn' --output json
