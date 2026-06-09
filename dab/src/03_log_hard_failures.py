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

# COMMAND ----------

import json

hard_json = dbutils.jobs.taskValues.get(
    taskKey="try_serverless",
    key="hard_failures",
    default="[]",
    debugValue="[]",
)
hard_failures = json.loads(hard_json)

print(f"{len(hard_failures)} hard (non-OOM) failure(s) -- logged, not retried, run not failed:\n")
for hf in hard_failures:
    print(f"  {hf['table']}")
    print(f"    {hf['error'][:300]}\n")

# COMMAND ----------

# Log-only by design: the task succeeds so the run is not marked failed.
# To make hard failures fail the run instead, uncomment:
# if hard_failures:
#     raise RuntimeError(
#         f"{len(hard_failures)} non-OOM failure(s) need manual review: "
#         f"{[hf['table'] for hf in hard_failures]}"
#     )
