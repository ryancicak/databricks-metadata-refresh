# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Classic fallback
# MAGIC
# MAGIC Reads the OOM-class table list from `try_serverless` and retries just
# MAGIC those tables on this classic cluster, which starts with the static
# MAGIC `spark.task.cpus=8` conf that serverless cannot set. Only runs when the
# MAGIC upstream condition task found `failed_count > 0`.
# MAGIC
# MAGIC ## Where the OOM list comes from
# MAGIC
# MAGIC `try_serverless` writes the OOM table list to a **per-run DBFS JSON file**
# MAGIC (not a task value - the list could be thousands of tables and would blow
# MAGIC the task-value size cap). We read that file here via `dbutils.fs`, which
# MAGIC works on this classic cluster exactly as it does on serverless.
# MAGIC
# MAGIC ## Completeness guarantee (no OOM table is ever silently dropped)
# MAGIC
# MAGIC A dropped OOM table = a table that silently never gets refreshed, which is
# MAGIC unacceptable. So we get the list from whichever source is authoritative and
# MAGIC available, and we cross-check the count:
# MAGIC
# MAGIC  1. Read the DBFS handoff file (the normal path - always complete).
# MAGIC  2. If that file is missing/unreadable but a `failure_log_table` is
# MAGIC     configured, re-derive the complete list from the log: every row this
# MAGIC     run tagged `action='oom_retry_classic'`. The producer writes the log
# MAGIC     before the handoff, so the log is the durable fallback.
# MAGIC  3. Whatever we end up with, compare its length to `failed_count` (the
# MAGIC     bounded task-value the producer always sets). If they disagree we FAIL
# MAGIC     this task loudly rather than refresh a partial list - a visible failure
# MAGIC     is recoverable; a silent drop is not.

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

dbutils.widgets.dropdown("mode", "live", ["live", "simulate"], "Run mode")
MODE = dbutils.widgets.get("mode")
dbutils.widgets.text("failure_log_table", "", "Failure log table (catalog.schema.table)")
LOG_TABLE = dbutils.widgets.get("failure_log_table").strip()
dbutils.widgets.text("run_id", "manual", "Job run id (auto-filled)")
RUN_ID = dbutils.widgets.get("run_id")

# COMMAND ----------

# Bounded scalars the producer always sets, even if its DBFS write failed. These
# carry zero per-table data, so they are immune to the task-value size cap.
EXPECTED_OOM_COUNT = dbutils.jobs.taskValues.get(
    taskKey="try_serverless", key="failed_count", default=0, debugValue=0
)
OOM_HANDOFF_DIR = dbutils.jobs.taskValues.get(
    taskKey="try_serverless", key="oom_handoff_dir", default="", debugValue=""
)
# Fall back to the producer's log table / run id if our own widgets are blank
# (e.g. this notebook run by hand). The producer publishes both as task values.
if not LOG_TABLE:
    LOG_TABLE = dbutils.jobs.taskValues.get(
        taskKey="try_serverless", key="failure_log_table", default="", debugValue=""
    )
if RUN_ID in ("", "manual"):
    RUN_ID = dbutils.jobs.taskValues.get(
        taskKey="try_serverless", key="run_id", default=RUN_ID, debugValue=RUN_ID
    )

# COMMAND ----------

# Read a sharded DBFS handoff dir back into the original object, via dbutils.fs
# (works on classic AND serverless; the /dbfs FUSE mount does not exist on
# serverless). Raises if the dir or manifest is missing/unreadable so the caller
# can fall back to the durable log rather than silently see an empty list.
def read_handoff(handoff_dir: str):
    manifest = json.loads(dbutils.fs.head(f"{handoff_dir}/_manifest.json"))
    parts = int(manifest["parts"])
    buf = []
    for idx in range(parts):
        # head() returns the whole file here: each shard was written under the
        # put() cap, and we ask for more than that cap so nothing is truncated.
        buf.append(dbutils.fs.head(f"{handoff_dir}/part-{idx:05d}.json", 1 << 30))
    return json.loads("".join(buf))


# Re-derive the complete OOM list from the durable log for this run. Used only
# when the DBFS handoff is unreadable; the producer writes the log BEFORE the
# handoff, so the log is the authoritative backup.
def oom_tables_from_log(log_table: str, run_id: str):
    from pyspark.sql.functions import col
    rows = (
        spark.table(log_table)
        .where(col("action") == "oom_retry_classic")
        .where(col("run_id") == run_id)  # parameterized: no run_id string interpolation
        .select("table_name")
        .distinct()
        .collect()
    )
    return [r["table_name"] for r in rows]


# COMMAND ----------

# Get the OOM list: DBFS handoff first, durable log as the completeness backup.
tables_to_retry = None
source = None
if OOM_HANDOFF_DIR:
    try:
        tables_to_retry = read_handoff(OOM_HANDOFF_DIR)
        source = f"DBFS handoff ({OOM_HANDOFF_DIR})"
    except Exception as e:  # noqa: BLE001
        print(f"WARN: could not read DBFS handoff {OOM_HANDOFF_DIR}: {type(e).__name__}: {e}")

if tables_to_retry is None and LOG_TABLE:
    try:
        tables_to_retry = oom_tables_from_log(LOG_TABLE, RUN_ID)
        source = f"durable log {LOG_TABLE} (run_id={RUN_ID})"
        print(f"Recovered OOM list from {source} - DBFS handoff was unavailable.")
    except Exception as e:  # noqa: BLE001
        print(f"WARN: could not recover OOM list from log {LOG_TABLE}: {type(e).__name__}: {e}")

if tables_to_retry is None:
    # No DBFS file and no usable log. If the producer also recorded zero OOM
    # tables, there is genuinely nothing to do; otherwise we must NOT proceed
    # with an empty list and silently skip real OOM tables.
    if EXPECTED_OOM_COUNT == 0:
        tables_to_retry = []
        source = "none (producer reported 0 OOM tables)"
    else:
        raise RuntimeError(
            f"OOM retry list is unavailable from both the DBFS handoff and the "
            f"durable log, but try_serverless reported {EXPECTED_OOM_COUNT} OOM "
            f"table(s). Refusing to run a partial fallback (a dropped OOM table "
            f"is silently never refreshed). Set failure_log_table to enable the "
            f"log-based recovery path, then re-run."
        )

# Completeness cross-check: the list we are about to retry must match the count
# the producer published. A mismatch means something truncated the list -- fail
# loudly rather than silently refresh a subset.
if len(tables_to_retry) != EXPECTED_OOM_COUNT:
    raise RuntimeError(
        f"OOM list completeness check FAILED: got {len(tables_to_retry)} table(s) "
        f"from {source} but try_serverless reported {EXPECTED_OOM_COUNT}. Refusing "
        f"to run a partial fallback. Inspect the DBFS handoff and the durable log "
        f"for run_id={RUN_ID}."
    )

print(f"Picked up {len(tables_to_retry)} OOM-class table(s) from {source}:")
for t in tables_to_retry:
    print(f"  {t}")

# COMMAND ----------


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
# a failure on one never stopped the others). Like the producer's handoff, the
# per-table list goes to DBFS and only the bounded COUNT goes on a task value --
# if every retried OOM table still failed, a full {table, error} list on a task
# value would hit the same "task value is too large" cap. The full list is in
# the DBFS file and (when configured) the durable log below.
classic_still_failing_dir = ""
try:
    payload = json.dumps(still_failing)
    d = f"dbfs:/tmp/metadata_refresh/handoff/{RUN_ID}/classic_still_failing"
    try:
        dbutils.fs.rm(d, recurse=True)
    except Exception:  # noqa: BLE001
        pass
    dbutils.fs.mkdirs(d)
    # 50,000-char shards stay under the 64 KiB dbutils.fs.head read cap (json.dumps
    # is ASCII, 1 byte/char), so any reader gets whole shards, never truncated.
    chunks = [payload[i:i + 50_000] for i in range(0, len(payload), 50_000)] or [""]
    for idx, ch in enumerate(chunks):
        dbutils.fs.put(f"{d}/part-{idx:05d}.json", ch, overwrite=True)
    dbutils.fs.put(f"{d}/_manifest.json", json.dumps({"parts": len(chunks)}), overwrite=True)
    # Read the shards back the same way readers do (dbutils.fs.head) and verify the
    # payload round-trips, matching the producer's self-check in 01_try_serverless.
    readback = "".join(
        dbutils.fs.head(f"{d}/part-{idx:05d}.json", 1 << 30) for idx in range(len(chunks))
    )
    if readback != payload:
        raise RuntimeError(f"still-failing handoff self-check failed for {d}: read-back mismatch (shard truncated?)")
    classic_still_failing_dir = d
except Exception as e:  # noqa: BLE001 -- recording must not fail the task
    print(f"WARN: could not write still-failing handoff: {type(e).__name__}: {e}")

dbutils.jobs.taskValues.set("classic_still_failing_count", len(still_failing))
dbutils.jobs.taskValues.set("classic_still_failing_dir", classic_still_failing_dir)

# Append the still-failing tables to the durable log too (action=classic_still_failing).
# Log-only: a logging problem must not fail the task.
if LOG_TABLE and still_failing:
    try:
        from pyspark.sql import functions as F
        # reason="oom": these are OOM-class tables that classic also could not fix.
        rows = [(r["table"], "classic_still_failing", "oom", r["error"][:4000]) for r in still_failing]
        (
            spark.createDataFrame(rows, "table_name string, action string, reason string, error_message string")
            .withColumn("run_id", F.lit(RUN_ID))
            .withColumn("mode", F.lit(MODE))
            .withColumn("logged_at", F.current_timestamp())
            .select("logged_at", "run_id", "mode", "table_name", "action", "reason", "error_message")
            .write.mode("append").option("mergeSchema", "true").saveAsTable(LOG_TABLE)
        )
        print(f"Logged {len(rows)} still-failing table(s) to {LOG_TABLE}.")
    except Exception as e:  # noqa: BLE001 -- logging must not fail the task
        print(f"WARN: could not write durable log to {LOG_TABLE}: {type(e).__name__}: {e}")

# Log-only: surface the result, do not fail the run. To make still-failing
# tables fail the run instead, uncomment the raise below. The preview is capped
# so a run with thousands of still-failing tables can't emit a giant single line;
# the full list is in classic_still_failing_dir (DBFS) and the durable log.
if still_failing:
    _preview = [r["table"] for r in still_failing[:20]]
    _more = f" (+{len(still_failing) - 20} more)" if len(still_failing) > 20 else ""
    print(
        f"\n!! {len(still_failing)} table(s) failed on BOTH serverless and classic "
        f"(logged, run not failed): {_preview}{_more}"
    )
    print(f"   full list: {classic_still_failing_dir or '(handoff write failed; see log table)'}")
    # raise RuntimeError(f"{len(still_failing)} table(s) failed on both serverless "
    #                    f"and classic; see {classic_still_failing_dir}")
else:
    print(f"All {len(tables_to_retry)} fallback refresh(es) succeeded on classic.")
