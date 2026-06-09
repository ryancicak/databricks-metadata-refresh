#!/bin/bash
# fix_lf_grant.sh — run as YOU:  bash ~/Documents/dbx-workspace-and-emr-iceberg/oom_demo/fix_lf_grant.sh
#
# Why the foreign catalog is empty: oom_demo's S3 location is NOT registered with
# Lake Formation, so LF rejects grants on its database/tables ("Insufficient Glue
# permissions ...") — which is why even your full-admin grant failed. This:
#   1. registers the location with LF,
#   2. grants DESCRIBE/SELECT to the Databricks federation role + you,
#   3. ALSO opens IAM/hybrid access (IAMAllowedPrincipals) as a fallback,
#   4. verifies. Idempotent; keeps going past "already exists".
cd "$(dirname "$0")/.." || exit 1
set -a; . ./aws_credentials.txt; set +a
REGION=us-west-2; ACCT=000000000000
ROLE="arn:aws:iam::${ACCT}:role/your-databricks-uc-role"
ME="arn:aws:iam::${ACCT}:user/your-iam-user"
LOC="arn:aws:s3:::your-databricks-rootbucket/oom_demo"
sec(){ echo; echo "==================== $* ===================="; }

sec "1/4  Register oom_demo S3 location with Lake Formation"
aws lakeformation register-resource --region $REGION --resource-arn "$LOC" --role-arn "$ROLE" \
  && echo "  registered $LOC (role: your-databricks-uc-role)" \
  || echo "  (register non-zero — likely already registered; continuing)"

sec "2/4  Grant DESCRIBE (db) + SELECT/DESCRIBE (all tables) to Databricks role + you"
for PR in "$ROLE" "$ME"; do
  aws lakeformation grant-permissions --region $REGION --principal DataLakePrincipalIdentifier="$PR" \
    --resource '{"Database":{"Name":"oom_demo"}}' --permissions DESCRIBE \
    && echo "  db DESCRIBE  -> $PR" || echo "  FAILED db DESCRIBE -> $PR"
  aws lakeformation grant-permissions --region $REGION --principal DataLakePrincipalIdentifier="$PR" \
    --resource '{"Table":{"DatabaseName":"oom_demo","TableWildcard":{}}}' --permissions SELECT DESCRIBE \
    && echo "  tbl SEL/DESC -> $PR" || echo "  FAILED tbl SEL/DESC -> $PR"
done

sec "3/4  Fallback: open IAM/hybrid access (IAMAllowedPrincipals = ALL)"
aws lakeformation grant-permissions --region $REGION \
  --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
  --resource '{"Database":{"Name":"oom_demo"}}' --permissions ALL \
  && echo "  IAMAllowedPrincipals ALL on db" || echo "  (IAMAllowedPrincipals db grant non-zero)"
aws lakeformation grant-permissions --region $REGION \
  --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
  --resource '{"Table":{"DatabaseName":"oom_demo","TableWildcard":{}}}' --permissions ALL \
  && echo "  IAMAllowedPrincipals ALL on tables" || echo "  (IAMAllowedPrincipals table grant non-zero)"

sec "4/4  Verify"
echo "-- LF permissions on oom_demo now:"
aws lakeformation list-permissions --region $REGION --resource '{"Database":{"Name":"oom_demo"}}' \
  --query 'PrincipalResourcePermissions[].{P:Principal.DataLakePrincipalIdentifier,Perms:Permissions}' --output json
echo "-- can you describe oom_demo now? (expect the name, not an error)"
aws glue get-database --name oom_demo --region $REGION --query 'Database.Name' --output text

sec "DONE — reply 'fixed' to Claude; it will REFRESH the schema, read hello, then run the OOM test."
