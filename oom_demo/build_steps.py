"""Emit an EMR add-steps JSON array for a spark-submit of oom_demo.py.
Usage: python3 build_steps.py <mode> [extra args...]  > /tmp/steps.json
"""
import json
import sys

BUCKET = "your-databricks-rootbucket"
SCRIPT = f"s3://{BUCKET}/oom_demo/scripts/oom_demo.py"
WAREHOUSE = f"s3://{BUCKET}/oom_demo/warehouse"

mode_args = sys.argv[1:] or ["hello"]

conf = [
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.type=glue",
    f"spark.sql.catalog.glue_catalog.warehouse={WAREHOUSE}",
    "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.glue_catalog.client.region=us-west-2",
]

args = [
    "spark-submit", "--deploy-mode", "client",
    # the fat per-file metrics overflow the 1GB default; lift it + grow the driver
    # (this is exactly the DAB's classic-cluster recipe: maxResultSize=0).
    "--driver-memory", "20g",
    "--conf", "spark.driver.maxResultSize=0",
]
for c in conf:
    args += ["--conf", c]
args += [SCRIPT] + mode_args

steps = [{
    "Name": "oom_demo_" + "_".join(mode_args)[:40],
    "Type": "CUSTOM_JAR",
    "ActionOnFailure": "CONTINUE",
    "Jar": "command-runner.jar",
    "Args": args,
}]
print(json.dumps(steps))
