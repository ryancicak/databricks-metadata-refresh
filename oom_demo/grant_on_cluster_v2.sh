#!/bin/bash
# grant_on_cluster_v2.sh — runs ON the EMR cluster as the creator role (the only
# principal LF lets grant on oom_demo). Grants table-level DESCRIBE/SELECT to the
# Databricks federation role + your-iam-user, captures every result, and uploads a
# report to S3 so the driver can read exactly what landed / failed.
R=us-west-2
BUCKET=your-databricks-rootbucket
OUT=/tmp/grant_result.txt
: > "$OUT"
DBX="arn:aws:iam::000000000000:role/your-databricks-uc-role"
ME="arn:aws:iam::000000000000:user/your-iam-user"

g() {  # $1=principal  $2=resource-json  $3..=perms
  local pr="$1" res="$2"; shift 2
  echo ">>> grant [$*] to $pr  on $res" >> "$OUT"
  if aws lakeformation grant-permissions --region "$R" \
        --principal DataLakePrincipalIdentifier="$pr" \
        --resource "$res" --permissions "$@" >> "$OUT" 2>&1; then
    echo "    OK" >> "$OUT"
  else
    echo "    FAILED(rc=$?)" >> "$OUT"
  fi
}

for PR in "$DBX" "$ME"; do
  g "$PR" '{"Database":{"Name":"oom_demo"}}' DESCRIBE
  g "$PR" '{"Table":{"DatabaseName":"oom_demo","TableWildcard":{}}}' SELECT DESCRIBE
done

{
  echo "=== list-permissions on TABLE oom_big ==="
  aws lakeformation list-permissions --region "$R" \
    --resource '{"Table":{"DatabaseName":"oom_demo","Name":"oom_big"}}' \
    --query 'PrincipalResourcePermissions[].{P:Principal.DataLakePrincipalIdentifier,Perms:Permissions}' --output json
  echo "=== get-tables as creator role ==="
  aws glue get-tables --database-name oom_demo --region "$R" --query 'TableList[].Name' --output json
} >> "$OUT" 2>&1

aws s3 cp "$OUT" "s3://$BUCKET/oom_demo/scripts/grant_result.txt" --region "$R"
echo GRANTS_V2_DONE
