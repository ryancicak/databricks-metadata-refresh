# Databricks notebook source
# Reproduce the DAB's log_failures write on serverless with a REAL hard-failure row,
# to capture why the live-path write fails where simulate succeeds.
import json
from pyspark.sql import functions as F

LOG = "your_catalog.default.metadata_refresh_failures"
result = {}

# 0) refresh the scope like the LIVE DAB does — incl the heavy 9.2GB oom_big —
#    BEFORE the log write, to test whether that stresses the executor.
for t in ["oom_cal", "oom_big"]:
    try:
        spark.sql(f"REFRESH FOREIGN TABLE your_foreign_catalog.oom_demo.{t}")
        result[f"refresh_{t}"] = "ok"
    except Exception as e:
        result[f"refresh_{t}"] = "ERR: " + str(e)[:100]

# 1) get a real REFRESH error (same as the DAB's does_not_exist hard failure)
try:
    spark.sql("REFRESH FOREIGN TABLE your_foreign_catalog.oom_demo.does_not_exist")
    result["refresh"] = "no error (unexpected)"
    msg = ""
except Exception as e:
    msg = str(e)
    result["refresh_err_len"] = len(msg)
    result["refresh_err_head"] = msg[:160]

# 2) replicate the exact log_failures write path
try:
    rows = [("your_foreign_catalog.oom_demo.does_not_exist", "hard_skip", msg[:4000])]
    df = (
        spark.createDataFrame(rows, "table_name string, action string, error_message string")
        .withColumn("run_id", F.lit("diag-test-v2"))
        .withColumn("mode", F.lit("live"))
        .withColumn("logged_at", F.current_timestamp())
        .select("logged_at", "run_id", "mode", "table_name", "action", "error_message")
    )
    df.write.mode("append").saveAsTable(LOG)
    result["write"] = "SUCCESS"
except Exception as e2:
    result["write"] = "FAILED"
    result["write_err"] = str(e2)[:1800]

dbutils.notebook.exit(json.dumps(result))
