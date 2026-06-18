# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Log hard failures
# MAGIC
# MAGIC Only runs when `try_serverless` reported `hard_failure_count > 0`. These
# MAGIC are non-OOM failures (bad name, permissions, schema) that classic compute
# MAGIC would not fix, so they are surfaced here for visibility.
# MAGIC
# MAGIC **This task only logs -- it does not fail the run.** The failures are also
# MAGIC captured in `try_serverless` (printed there, and appended to
# MAGIC `failure_log_table` if configured). If you would rather be paged, wire a
# MAGIC job notification to this task or re-enable the raise at the bottom.
# MAGIC
# MAGIC The hard-failure list is read from the **per-run DBFS handoff file**
# MAGIC `try_serverless` writes (not a task value: the list can be most of ~200
# MAGIC tables when federation cannot read merge-on-read tables, which is exactly
# MAGIC what blew the task-value size cap before). This task is log-only, so an
# MAGIC unreadable handoff degrades to "nothing to print" and never fails the run.

# COMMAND ----------

import json

# Bounded count the producer always sets (independent of how many tables failed).
hard_count = dbutils.jobs.taskValues.get(
    taskKey="try_serverless", key="hard_failure_count", default=0, debugValue=0
)
hard_dir = dbutils.jobs.taskValues.get(
    taskKey="try_serverless", key="hard_handoff_dir", default="", debugValue=""
)
# Published by try_serverless so this task can recover the detail from the durable
# log if the DBFS handoff is ever unreadable (e.g. DBFS root disabled).
LOG_TABLE = dbutils.jobs.taskValues.get(
    taskKey="try_serverless", key="failure_log_table", default="", debugValue=""
)
RUN_ID = dbutils.jobs.taskValues.get(
    taskKey="try_serverless", key="run_id", default="manual", debugValue="manual"
)


def read_handoff(handoff_dir: str):
    """Read a sharded DBFS handoff dir back into its object, via dbutils.fs
    (works on serverless and classic; /dbfs FUSE does not exist on serverless)."""
    manifest = json.loads(dbutils.fs.head(f"{handoff_dir}/_manifest.json"))
    buf = [
        dbutils.fs.head(f"{handoff_dir}/part-{idx:05d}.json", 1 << 30)
        for idx in range(int(manifest["parts"]))
    ]
    return json.loads("".join(buf))


hard_failures = []
if hard_dir:
    try:
        hard_failures = read_handoff(hard_dir)
    except Exception as e:  # noqa: BLE001 -- log-only task, never fail the run
        print(f"WARN: could not read hard-failure handoff {hard_dir}: {type(e).__name__}: {e}")

# Fallback: if the DBFS handoff was missing/unreadable but a durable log is
# configured, recover the full detail from it (rows this run tagged hard_skip).
# Still log-only -- any problem here just prints a warning, never fails the run.
if not hard_failures and hard_count and LOG_TABLE:
    try:
        from pyspark.sql.functions import col
        tbl = spark.table(LOG_TABLE)
        # reason is optional: a log table written by a pre-reason version won't have it.
        cols = ["table_name", "error_message"] + (["reason"] if "reason" in tbl.columns else [])
        rows = (
            tbl
            .where(col("action") == "hard_skip")
            .where(col("run_id") == RUN_ID)
            .select(*cols)
            .distinct()  # a Databricks task retry re-appends rows under the same run_id; dedup so the count matches 02
            .collect()
        )
        hard_failures = [
            {"table": r["table_name"], "error": r["error_message"] or "",
             "reason": (r["reason"] if "reason" in cols and r["reason"] else "other")}
            for r in rows
        ]
        if hard_failures:
            print(f"recovered {len(hard_failures)} hard failure(s) from {LOG_TABLE}")
    except Exception as e:  # noqa: BLE001
        print(f"WARN: could not read hard failures from log {LOG_TABLE}: {type(e).__name__}: {e}")

if not hard_failures and hard_count:
    print(f"NOTE: producer reported {hard_count} hard failure(s) but the detail was not "
          "recoverable from the DBFS handoff or the log; see try_serverless output for the per-table lines.")

# Per-reason breakdown first (bounded), so the operator sees WHY before the list.
if hard_failures:
    from collections import Counter
    _by_reason = Counter(hf.get("reason", "other") for hf in hard_failures)
    print("hard-failure reasons:")
    for _reason, _n in _by_reason.most_common():
        print(f"  {_reason:24s}: {_n}")
    print()

_PREVIEW = 50  # cap the print so a run with 100k hard failures can't flood output
print(f"{len(hard_failures)} hard (non-OOM) failure(s) -- logged, not retried, run not failed:\n")
for hf in hard_failures[:_PREVIEW]:
    print(f"  [{hf.get('reason', 'other')}] {hf['table']}")
    print(f"    {str(hf.get('error', ''))[:300]}\n")
if len(hard_failures) > _PREVIEW:
    print(f"  ... and {len(hard_failures) - _PREVIEW} more (full list in the DBFS handoff "
          "and, if configured, failure_log_table).")

# COMMAND ----------

# Log-only by design: the task succeeds so the run is not marked failed.
# To make hard failures fail the run instead, uncomment:
# if hard_failures:
#     raise RuntimeError(
#         f"{len(hard_failures)} non-OOM failure(s) need manual review: "
#         f"{[hf['table'] for hf in hard_failures]}"
#     )
