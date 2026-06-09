# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Serverless refresh pass
# MAGIC
# MAGIC Runs `REFRESH FOREIGN TABLE` over every table in the scope list on
# MAGIC serverless, catching each table's error and sorting it into one of two
# MAGIC buckets:
# MAGIC
# MAGIC | bucket | meaning | next step |
# MAGIC |---|---|---|
# MAGIC | **OOM-class** | ran out of memory / lost an executor | retry on the classic cluster (has `spark.task.cpus=8`) |
# MAGIC | **hard failure** | bad name, permissions, schema, etc. | alert only -- classic would fail too |
# MAGIC
# MAGIC The two lists are handed to downstream tasks via `dbutils.jobs.taskValues`.

# COMMAND ----------

# MAGIC %run ./scope_tables

# COMMAND ----------

import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## OOM detection
# MAGIC
# MAGIC The OOM errors these tables throw are **not identical** -- the same
# MAGIC underlying memory pressure surfaces under several different Databricks
# MAGIC error classes and reasons. Rather than match each exact string, we match
# MAGIC the family of tokens that all of them share. Each pattern below maps to a
# MAGIC real failure seen on these tables:
# MAGIC
# MAGIC ```
# MAGIC [JVM_OUT_OF_MEMORY] ... java.lang.OutOfMemoryError: GC overhead limit exceeded
# MAGIC [TASK_FAILED_EXECUTOR_LOSS] ... ExecutorLostFailure ... Reason: Remote RPC client disassociated ...
# MAGIC [TASK_FAILED_EXECUTOR_LOSS] ... ExecutorLostFailure ... Reason: Command exited with code 52, oom ...
# MAGIC [TASK_FAILED_EXECUTOR_LOSS] ... ExecutorLostFailure ... Reason: heartbeat_timeout, unknown cause ...
# MAGIC Job aborted due to stage failure: java.lang.OutOfMemoryError: GC overhead limit exceeded
# MAGIC ```
# MAGIC
# MAGIC If a new OOM variant shows up later, add one token here -- you do not need
# MAGIC the full message.

# COMMAND ----------

# Any of these tokens in an error message => OOM-class => retry on classic.
# IGNORECASE + DOTALL so multi-line stack traces match.
RETRY_PATTERNS = [
    re.compile(p, re.IGNORECASE | re.DOTALL)
    for p in [
        r"JVM_OUT_OF_MEMORY",            # [JVM_OUT_OF_MEMORY] error class
        r"OutOfMemoryError",             # java.lang.OutOfMemoryError (any cause)
        r"GC overhead limit exceeded",   # the most common JVM OOM cause here
        r"TASK_FAILED_EXECUTOR_LOSS",    # [TASK_FAILED_EXECUTOR_LOSS] error class
        r"ExecutorLostFailure",          # executor died mid-task
        r"executor\s+\d+\s+exited",      # "executor 141 exited caused by..."
        r"Command exited with code 52",  # OOM-killer exit code
        r"\boom\b",                      # bare "oom" in the reason text
        r"remote rpc client disassociated",  # container exceeded memory thresholds
        r"heartbeat_timeout",            # executor stopped heartbeating (mem starvation)
    ]
]


def is_oom_class(err_msg: str) -> bool:
    """True if the error looks like memory pressure worth retrying on classic."""
    return any(p.search(err_msg) for p in RETRY_PATTERNS)


# Quick self-check against the exact error families we expect to retry.
_SANITY = [
    "[JVM_OUT_OF_MEMORY] Query failed because of JVM out of memory exception: "
    "java.lang.OutOfMemoryError: GC overhead limit exceeded",
    "[TASK_FAILED_EXECUTOR_LOSS] Task failed due to executor loss: ExecutorLostFailure "
    "(executor 141 exited caused by one of the running tasks) Reason: Remote RPC client "
    "disassociated. ... SQLSTATE: XX000",
    "[TASK_FAILED_EXECUTOR_LOSS] Task failed due to executor loss: ExecutorLostFailure "
    "(executor 295 exited caused by one of the running tasks) Reason: Command exited with "
    "code 52, oom SQLSTATE: XX000",
    "[TASK_FAILED_EXECUTOR_LOSS] Task failed due to executor loss: ExecutorLostFailure "
    "(executor 340 exited caused by one of the running tasks) Reason: heartbeat_timeout, "
    "unknown cause SQLSTATE: XX000",
    "Job aborted due to stage failure: java.lang.OutOfMemoryError: GC overhead limit exceeded",
]
assert all(is_oom_class(m) for m in _SANITY), "OOM regex failed to match a known OOM error"
# And a hard failure must NOT match.
assert not is_oom_class("AnalysisException: Table or view 'foo' not found"), \
    "OOM regex wrongly matched a hard failure"
print("OOM classifier sanity check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run mode
# MAGIC
# MAGIC * **live**  -- actually issue `REFRESH FOREIGN TABLE` against each table.
# MAGIC * **simulate** -- do not touch any table; replay the real OOM and hard-failure
# MAGIC   error strings against a few sample tables so you can watch the classifier
# MAGIC   and the classic-fallback branch work end-to-end before going live.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "live", ["live", "simulate"], "Run mode")
MODE = dbutils.widgets.get("mode")
print(f"mode = {MODE}")

# Optional durable log. When set to a <catalog>.<schema>.<table>, every serverless
# failure (OOM-class AND hard) is appended there with the action taken, so there
# is a queryable record beyond the run's stdout/task-values. Blank = disabled.
dbutils.widgets.text("failure_log_table", "", "Failure log table (catalog.schema.table)")
LOG_TABLE = dbutils.widgets.get("failure_log_table").strip()
print(f"failure_log_table = {LOG_TABLE or '(disabled)'}")

# Run id for the durable log. Fed by the {{job.run_id}} base parameter -- read it
# from the widget, NOT spark.conf (serverless blocks spark.databricks.job.runId).
dbutils.widgets.text("run_id", "manual", "Job run id (auto-filled)")
RUN_ID = dbutils.widgets.get("run_id")

# In simulate mode, these tables throw canned errors; everything else "succeeds".
# Generic placeholder names -- the real error strings are what matters here.
SIMULATED_ERRORS = {
    # OOM-class -> should be retried on classic
    "catalog.schema.oom_table_jvm": (
        "[JVM_OUT_OF_MEMORY] Query failed because of JVM out of memory exception: "
        "java.lang.OutOfMemoryError: GC overhead limit exceeded\n"
        "\tat scala.collection.immutable.BitmapIndexedMapNode.mergeTwoKeyValPairs(HashMap.scala:928)"
    ),
    "catalog.schema.oom_table_executor_loss": (
        "[TASK_FAILED_EXECUTOR_LOSS] Task failed due to executor loss: ExecutorLostFailure "
        "(executor 295 exited caused by one of the running tasks) Reason: Command exited with "
        "code 52, oom SQLSTATE: XX000"
    ),
    "catalog.schema.oom_table_gc": (
        "Job aborted due to stage failure: java.lang.OutOfMemoryError: GC overhead limit exceeded"
    ),
    # hard failure -> should NOT be retried on classic
    "catalog.schema.missing_table": (
        "AnalysisException: Table or view 'missing_table' not found in catalog 'catalog'"
    ),
}


def refresh_one(name: str) -> None:
    """Refresh a single foreign table. Raises on failure."""
    if MODE == "simulate":
        if name in SIMULATED_ERRORS:
            raise RuntimeError(SIMULATED_ERRORS[name])
        return  # treated as a clean serverless success
    spark.sql(f"REFRESH FOREIGN TABLE {name}")

# COMMAND ----------

# Live runs the real scope list; simulate runs the canned sample tables (so it
# works and exercises both branches even without the real scope_tables.py).
if MODE == "simulate":
    tables_to_process = list(SIMULATED_ERRORS.keys()) + [
        "catalog.schema.clean_table_1",
        "catalog.schema.clean_table_2",
    ]
else:
    tables_to_process = SCOPE_TABLES

retry_on_classic = []   # OOM-class: hand to the classic fallback task
hard_failures = []      # everything else: alert, do not retry
failure_rows = []       # every failure, for the durable log: (table, action, error)
succeeded = 0

for t in tables_to_process:
    try:
        refresh_one(t)
        succeeded += 1
    except Exception as e:  # noqa: BLE001 -- we classify, not swallow
        msg = str(e)
        # One line per failed table -- only prints when a table actually fails,
        # and each error is truncated, so the output stays bounded.
        if is_oom_class(msg):
            print(f"  OOM-class (-> classic): {t} :: {msg[:160]}")
            retry_on_classic.append(t)
            failure_rows.append((t, "oom_retry_classic", msg[:4000]))
        else:
            print(f"  hard failure (-> logged, not retried): {t} :: {msg[:160]}")
            hard_failures.append({"table": t, "error": msg[:1000]})
            failure_rows.append((t, "hard_skip", msg[:4000]))

# Counts only -- the per-table detail above is the full list (no giant single-line
# dump, so a run with many failures can't blow up the output).
print("\nServerless pass complete.")
print(f"  succeeded on serverless        : {succeeded}")
print(f"  OOM-class (retried on classic) : {len(retry_on_classic)}")
print(f"  hard failures (logged, skipped): {len(hard_failures)}")

# COMMAND ----------

# Durable log: append every failure (both buckets) to the configured Delta table.
# action = "oom_retry_classic" (sent to classic) or "hard_skip" (logged, not retried).
# Logging must NEVER fail the refresh -- any write problem is caught and warned.
def log_failures(rows):
    if not LOG_TABLE:
        print("failure_log_table not set -- skipping durable log.")
        return
    if not rows:
        print(f"No failures to log to {LOG_TABLE}.")
        return
    try:
        from pyspark.sql import functions as F
        df = (
            spark.createDataFrame(rows, "table_name string, action string, error_message string")
            .withColumn("run_id", F.lit(RUN_ID))
            .withColumn("mode", F.lit(MODE))
            .withColumn("logged_at", F.current_timestamp())
            .select("logged_at", "run_id", "mode", "table_name", "action", "error_message")
        )
        df.write.mode("append").saveAsTable(LOG_TABLE)
        print(f"Logged {len(rows)} failure(s) to {LOG_TABLE}.")
    except Exception as e:  # noqa: BLE001 -- logging must not fail the refresh
        print(f"WARN: could not write durable log to {LOG_TABLE}: {type(e).__name__}: {e}")


log_failures(failure_rows)

# COMMAND ----------

# Wire the results to the downstream tasks. These are the only thing passed
# between tasks -- small JSON blobs read by taskKey + key.
dbutils.jobs.taskValues.set("failed_tables", json.dumps(retry_on_classic))
dbutils.jobs.taskValues.set("failed_count", len(retry_on_classic))
dbutils.jobs.taskValues.set("hard_failures", json.dumps(hard_failures))
dbutils.jobs.taskValues.set("hard_failure_count", len(hard_failures))
