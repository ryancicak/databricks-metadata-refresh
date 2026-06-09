#!/bin/bash
# fix_lf_via_emr.sh — run once:  !bash ~/Documents/dbx-workspace-and-emr-iceberg/oom_demo/fix_lf_via_emr.sh
#
# Grants Lake Formation perms on oom_demo FROM the EMR instance role — the database's
# creator, and the only principal LF will let grant on it (even your full-admin user
# can't, due to an LF/Glue-Iceberg quirk). Steps: (1) add LF-grant IAM to the EMR role,
# (2) run the grant AS the EMR role via a cluster step, (3) poll, (4) verify.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
set -a; . ./aws_credentials.txt; set +a
REGION=us-west-2; ACCT=000000000000; CLUSTER=j-YOUREMRCLUSTERID
BUCKET=your-databricks-rootbucket; EMR_ROLE=your-emr-instance-role
sec(){ echo; echo "==================== $* ===================="; }

sec "1/4  Give the EMR role permission to call Lake Formation grant APIs"
aws iam put-role-policy --role-name "$EMR_ROLE" --policy-name lf-grant \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["lakeformation:GrantPermissions","lakeformation:BatchGrantPermissions","lakeformation:ListPermissions","lakeformation:GetDataAccess"],"Resource":"*"}]}' \
  && echo "  ok — added lf-grant to $EMR_ROLE; waiting 15s for IAM propagation" && sleep 15

sec "2/4  Stage the grant script (executes on the cluster, as the creator role)"
cat > /tmp/grant_on_cluster.sh <<'EOS'
#!/bin/bash
set -x
R=us-west-2
DBX="arn:aws:iam::000000000000:role/your-databricks-uc-role"
ME="arn:aws:iam::000000000000:user/your-iam-user"
for PR in "$DBX" "$ME"; do
  aws lakeformation grant-permissions --region $R --principal DataLakePrincipalIdentifier="$PR" \
    --resource '{"Database":{"Name":"oom_demo"}}' --permissions DESCRIBE
  aws lakeformation grant-permissions --region $R --principal DataLakePrincipalIdentifier="$PR" \
    --resource '{"Table":{"DatabaseName":"oom_demo","TableWildcard":{}}}' --permissions SELECT DESCRIBE
done
aws lakeformation grant-permissions --region $R --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
  --resource '{"Database":{"Name":"oom_demo"}}' --permissions ALL
aws lakeformation grant-permissions --region $R --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
  --resource '{"Table":{"DatabaseName":"oom_demo","TableWildcard":{}}}' --permissions ALL
echo GRANTS_DONE
EOS
aws s3 cp /tmp/grant_on_cluster.sh "s3://$BUCKET/oom_demo/scripts/grant_on_cluster.sh" --region $REGION && echo "  staged"

sec "3/4  Run the grant as the EMR role (cluster step) + poll"
cat > /tmp/grant_step.json <<EOS
[{"Name":"lf_grant","Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Args":["bash","-c","aws s3 cp s3://$BUCKET/oom_demo/scripts/grant_on_cluster.sh /tmp/g.sh && bash /tmp/g.sh"]}]
EOS
STEP=$(aws emr add-steps --cluster-id $CLUSTER --region $REGION --steps file:///tmp/grant_step.json --query 'StepIds[0]' --output text)
echo "  step $STEP — polling (about a minute)..."
while true; do
  S=$(aws emr describe-step --cluster-id $CLUSTER --step-id "$STEP" --region $REGION --query 'Step.Status.State' --output text)
  echo "    $S"
  case "$S" in COMPLETED|FAILED|CANCELLED|INTERRUPTED) break;; esac
  sleep 15
done

sec "4/4  Verify (as you)"
echo "-- LF permissions on oom_demo now (expect your-databricks-uc-role / IAM_ALLOWED_PRINCIPALS):"
aws lakeformation list-permissions --region $REGION --resource '{"Database":{"Name":"oom_demo"}}' \
  --query 'PrincipalResourcePermissions[].{P:Principal.DataLakePrincipalIdentifier,Perms:Permissions}' --output json
echo "-- can you describe oom_demo now? (expect: oom_demo)"
aws glue get-database --name oom_demo --region $REGION --query 'Database.Name' --output text

sec "DONE — reply 'fixed' to Claude; it runs the hello proof + the OOM test (no more scripts for you)."
