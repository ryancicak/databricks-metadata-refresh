#!/bin/bash
# setup_federation.sh
# Run as YOU (the user) via:  ! bash ~/Documents/dbx-workspace-and-emr-iceberg/oom_demo/setup_federation.sh
#
# Wires AWS Glue -> Databricks Unity Catalog federation so REFRESH FOREIGN TABLE works.
# These are the exact privileged steps the agent's auto-mode classifier blocks
# (LF grants, UC service credential, connection, foreign catalog). Running them
# here in YOUR shell clears that block. Idempotent: re-running is safe.
cd "$(dirname "$0")/.." || exit 1
set -a; . ./aws_credentials.txt; set +a

REGION=us-west-2
ACCT=000000000000
ROLE="arn:aws:iam::${ACCT}:role/your-databricks-uc-role"
ME="arn:aws:iam::${ACCT}:user/your-iam-user"
BUCKET=your-databricks-rootbucket
PROFILE=feast-demo
USER_EMAIL='you@example.com'
sec() { echo; echo "==================== $* ===================="; }

sec "1/6  Lake Formation grants on oom_demo (Databricks role + you)"
for PR in "$ROLE" "$ME"; do
  aws lakeformation grant-permissions --region $REGION \
    --principal DataLakePrincipalIdentifier="$PR" \
    --resource '{"Database":{"Name":"oom_demo"}}' --permissions DESCRIBE \
    && echo "  DESCRIBE db -> $PR"
  aws lakeformation grant-permissions --region $REGION \
    --principal DataLakePrincipalIdentifier="$PR" \
    --resource '{"Table":{"DatabaseName":"oom_demo","TableWildcard":{}}}' --permissions SELECT DESCRIBE \
    && echo "  SELECT/DESCRIBE tables -> $PR"
done

sec "2/6  UC service credential your_glue_service_credential (Glue catalog API)"
databricks credentials create-credential -p $PROFILE --json \
  "{\"name\":\"your_glue_service_credential\",\"purpose\":\"SERVICE\",\"aws_iam_role\":{\"role_arn\":\"${ROLE}\"}}" \
  || echo "  (create returned non-zero — may already exist)"
echo "  --> service credential external_id (must be trusted by the role):"
databricks credentials get-credential your_glue_service_credential -p $PROFILE -o json 2>/dev/null \
  | python3 -c "import sys,json;d=json.load(sys.stdin);print('     ',d.get('aws_iam_role',{}).get('external_id'))" 2>/dev/null \
  || echo "     (could not read external_id)"

sec "3/6  External location oom_demo_loc (the storage location for the data)"
databricks external-locations create -p $PROFILE --json \
  "{\"name\":\"oom_demo_loc\",\"url\":\"s3://${BUCKET}/oom_demo\",\"credential_name\":\"your-storage-credential\"}" \
  || echo "  (create returned non-zero — may already exist)"

sec "4/6  Connection glue_conn (Glue HMS federation)"
databricks connections create -p $PROFILE --json \
  "{\"name\":\"glue_conn\",\"connection_type\":\"GLUE\",\"options\":{\"aws_region\":\"${REGION}\",\"aws_account_id\":\"${ACCT}\",\"credential\":\"your_glue_service_credential\"}}" \
  || echo "  (create returned non-zero — may already exist, OR the credential external_id is not trusted by the role)"

sec "5/6  Foreign catalog your_foreign_catalog (authorized_paths = the data path)"
databricks catalogs create -p $PROFILE --json \
  "{\"name\":\"your_foreign_catalog\",\"connection_name\":\"glue_conn\",\"options\":{\"authorized_paths\":\"s3://${BUCKET}/oom_demo\"}}" \
  || echo "  (create returned non-zero — may already exist)"

sec "6/6  Hand ownership + grant to ${USER_EMAIL}"
databricks catalogs update your_foreign_catalog --json "{\"owner\":\"${USER_EMAIL}\"}" -p $PROFILE || true
databricks grants update catalog your_foreign_catalog --json \
  "{\"changes\":[{\"principal\":\"${USER_EMAIL}\",\"add\":[\"ALL_PRIVILEGES\"]}]}" -p $PROFILE || true
databricks connections update glue_conn --json "{\"owner\":\"${USER_EMAIL}\"}" -p $PROFILE || true
databricks external-locations update oom_demo_loc --json "{\"owner\":\"${USER_EMAIL}\"}" -p $PROFILE || true

sec "DONE"
echo "Federation objects created. Reply 'done' to Claude — it will verify, run the"
echo "small-table proof (SELECT ... hello), then the OOM test on oom_big."
