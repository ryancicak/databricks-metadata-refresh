# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Classic fallback
# MAGIC
# MAGIC Reads the OOM-class table list from `try_serverless` and retries just
# MAGIC those tables on this classic cluster, which starts with the static
# MAGIC `spark.task.cpus=8` conf that serverless cannot set. Only runs when the
# MAGIC upstream condition task found `failed_count > 0`.

# COMMAND ----------

import json

# COMMAND ----------

# Confirm the static Spark conf the OOM tables need is actually present.
# On the classic cluster this prints "8"; if it prints anything else the
# cluster spec drifted. Wrapped so the check itself can never fail the task --
# serverless blocks reading some static confs outright (raises CONFIG_NOT_AVAILABLE
# rather than returning a default), and this notebook should only ever run on the
# classic cluster anyway.
for k in ("spark.task.cpus", "spark.driver.maxResultSize"):
    try:
        print(f"  {k} = {spark.conf.get(k)}")
    except Exception as e:  # noqa: BLE001
        print(f"  {k} = <unavailable: {type(e).__name__}>")

# COMMAND ----------

# Pull the OOM-class list written by the serverless task.
failed_json = dbutils.jobs.taskValues.get(
    taskKey="try_serverless",
    key="failed_tables",
    default="[]",
    debugValue="[]",  # used only when running this notebook by hand
)
tables_to_retry = json.loads(failed_json)
print(f"Picked up {len(tables_to_retry)} OOM-class table(s) to retry on classic:")
for t in tables_to_retry:
    print(f"  {t}")

# COMMAND ----------

dbutils.widgets.dropdown("mode", "live", ["live", "simulate"], "Run mode")
MODE = dbutils.widgets.get("mode")
dbutils.widgets.text("failure_log_table", "", "Failure log table (catalog.schema.table)")
LOG_TABLE = dbutils.widgets.get("failure_log_table").strip()
dbutils.widgets.text("run_id", "manual", "Job run id (auto-filled)")
RUN_ID = dbutils.widgets.get("run_id")


def refresh_one_classic(name: str) -> None:
    """Refresh on classic, where the static conf gives these tables the heap
    they need. In simulate mode we just report success."""
    if MODE == "simulate":
        return
    spark.sql(f"REFRESH FOREIGN TABLE {name}")

# COMMAND ----------

still_failing = []
for t in tables_to_retry:
    try:
        refresh_one_classic(t)
        print(f"  refreshed on classic: {t}")
    except Exception as e:  # noqa: BLE001
        print(f"  STILL FAILED on classic: {t} :: {str(e)[:200]}")
        still_failing.append({"table": t, "error": str(e)[:1000]})

# COMMAND ----------

# Record what classic could not fix (every table was attempted independently;
# a failure on one never stopped the others).
dbutils.jobs.taskValues.set("classic_still_failing", json.dumps(still_failing))
dbutils.jobs.taskValues.set("classic_still_failing_count", len(still_failing))

# Append the still-failing tables to the durable log too (action=classic_still_failing).
# Log-only: a logging problem must not fail the task.
if LOG_TABLE and still_failing:
    try:
        from pyspark.sql import functions as F
        rows = [(r["table"], "classic_still_failing", r["error"][:4000]) for r in still_failing]
        (
            spark.createDataFrame(rows, "table_name string, action string, error_message string")
            .withColumn("run_id", F.lit(RUN_ID))
            .withColumn("mode", F.lit(MODE))
            .withColumn("logged_at", F.current_timestamp())
            .select("logged_at", "run_id", "mode", "table_name", "action", "error_message")
            .write.mode("append").saveAsTable(LOG_TABLE)
        )
        print(f"Logged {len(rows)} still-failing table(s) to {LOG_TABLE}.")
    except Exception as e:  # noqa: BLE001 -- logging must not fail the task
        print(f"WARN: could not write durable log to {LOG_TABLE}: {type(e).__name__}: {e}")

# Log-only: surface the result, do not fail the run. To make still-failing
# tables fail the run instead, uncomment the raise below.
if still_failing:
    print(
        f"\n!! {len(still_failing)} table(s) failed on BOTH serverless and classic "
        f"(logged, run not failed): {[r['table'] for r in still_failing]}"
    )
    # raise RuntimeError(f"{len(still_failing)} table(s) failed on both: "
    #                    f"{[r['table'] for r in still_failing]}")
else:
    print(f"All {len(tables_to_retry)} fallback refresh(es) succeeded on classic.")
