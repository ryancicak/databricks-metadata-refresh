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
# MAGIC ## How the two lists reach the downstream tasks
# MAGIC
# MAGIC The per-table lists (OOM tables to retry, hard failures to log) are written
# MAGIC to a **per-run JSON file on DBFS**, and `dbutils.jobs.taskValues` carries
# MAGIC only fixed-size scalars: the two counts (for the condition tasks) and the
# MAGIC DBFS handoff path. This is deliberate.
# MAGIC
# MAGIC `dbutils.jobs.taskValues` has a hard size cap. The original code put the
# MAGIC full `hard_failures` list (a `{table, error}` dict per failure) straight
# MAGIC into a task value, so a run where a big chunk of ~200 tables hard-failed at
# MAGIC once (Databricks foreign-Iceberg federation cannot read tables with
# MAGIC row-level deletes / merge-on-read, so many fail together) blew the cap with
# MAGIC `INVALID_PARAMETER_VALUE: The task value is too large`, failing the whole
# MAGIC run on the handoff line.
# MAGIC
# MAGIC Moving the *lists* off task values makes that error **structurally
# MAGIC impossible at any scale** (1, 200, 100k tables): what we set on task values
# MAGIC is now bounded - two integers and one short path string - and never grows
# MAGIC with the number of failures.
# MAGIC
# MAGIC ## Why DBFS (and not a Delta table / volume / Workspace file)
# MAGIC
# MAGIC The tasks run on **different compute** (this one + log_hard_failures on
# MAGIC serverless; fallback_classic on a classic cluster). They can only share data
# MAGIC via task values (too small) or a durable store both can reach. The store has
# MAGIC to work with **zero extra config** - `failure_log_table` is optional and may
# MAGIC be blank, so we cannot lean on it for the handoff.
# MAGIC
# MAGIC DBFS is the one location reachable from both serverless and classic with no
# MAGIC setup: no UC volume to create, no storage location, no `CREATE TABLE` grant.
# MAGIC We read/write it through `dbutils.fs` (and Spark), NOT the `/dbfs` FUSE mount
# MAGIC - FUSE/local-file paths are not available on serverless, but `dbutils.fs` is
# MAGIC available everywhere. The file is namespaced by `run_id`, so concurrent or
# MAGIC historical runs never collide (and `max_concurrent_runs=1` already serializes
# MAGIC runs on these tables anyway).
# MAGIC
# MAGIC The optional `failure_log_table` is unchanged and orthogonal: it is the
# MAGIC durable, queryable audit record. The DBFS file is just the intra-run handoff.

# COMMAND ----------

# MAGIC %run ./scope_tables

# COMMAND ----------

import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-run DBFS handoff
# MAGIC
# MAGIC The per-table lists are passed to the downstream tasks as a JSON file on
# MAGIC DBFS, addressed by `run_id`. Task values then only carry counts + this path,
# MAGIC so they can never exceed the size cap no matter how many tables fail.
# MAGIC
# MAGIC Access is via `dbutils.fs` (works on serverless AND classic), never the
# MAGIC `/dbfs` FUSE mount (not present on serverless). Writes are chunked to stay
# MAGIC under the `dbutils.fs.put` per-call string limit, so a 100k-table failure
# MAGIC list still writes fine. The matching reader is inlined in 02/03 with the
# MAGIC identical shard format (`part-NNNNN.json` + `_manifest.json {"parts": N}`).

# COMMAND ----------

# Root for the intra-run handoff files. DBFS is reachable from both serverless
# and classic with zero configuration (no volume, storage location, or grant).
HANDOFF_ROOT = "dbfs:/tmp/metadata_refresh/handoff"

# dbutils.fs.put takes the whole string in one call and caps it (~few hundred KB
# of UTF-8). A list of 100k {table, error[:1000]} dicts is far larger than that,
# so we shard the JSON into fixed-size files and write a tiny manifest that names
# the shards. Reading concatenates them back. Bytes per file stay bounded; the
# number of files grows with the data, never the size of any single put().
# Each shard must round-trip through BOTH dbutils.fs.put (write) AND dbutils.fs.head
# (the read used in 02/03). put tolerates a few hundred KB, but head silently caps
# every read at 64 KiB no matter what maxBytes you pass -- so a shard larger than
# 64 KiB is TRUNCATED on read and corrupts the reassembled JSON. json.dumps emits
# pure ASCII (ensure_ascii=True, 1 byte/char), so 50,000 chars keeps every shard
# safely under 64 KiB on both the write and the read side.
_HANDOFF_CHUNK_CHARS = 50_000


def _safe_run_id(run_id) -> str:
    """Keep only filesystem-safe chars so a stray run_id can't escape the dir."""
    return "".join(c for c in str(run_id) if c.isalnum() or c in ("-", "_")) or "manual"


def handoff_dir(run_id, name: str) -> str:
    """DBFS directory holding one handoff payload's manifest + shards."""
    return f"{HANDOFF_ROOT}/{_safe_run_id(run_id)}/{name}"


def write_handoff(run_id, name: str, obj) -> str:
    """Serialize obj to JSON and write it to DBFS as sharded files under a
    per-run, per-name directory. Returns the directory path (what downstream
    tasks read). Overwrites any prior content for this run+name so a manual
    re-run is idempotent."""
    payload = json.dumps(obj)
    d = handoff_dir(run_id, name)
    # Start clean so a re-run with fewer shards can't leave stale shards behind.
    try:
        dbutils.fs.rm(d, recurse=True)
    except Exception:  # noqa: BLE001 -- absent dir is fine
        pass
    dbutils.fs.mkdirs(d)
    chunks = [
        payload[i : i + _HANDOFF_CHUNK_CHARS]
        for i in range(0, len(payload), _HANDOFF_CHUNK_CHARS)
    ] or [""]  # empty payload still writes exactly one (empty) shard
    for idx, chunk in enumerate(chunks):
        # overwrite=True so a re-run cannot fail on a leftover shard file.
        dbutils.fs.put(f"{d}/part-{idx:05d}.json", chunk, overwrite=True)
    # Manifest is tiny and fixed-shape: just the shard count.
    dbutils.fs.put(f"{d}/_manifest.json", json.dumps({"parts": len(chunks)}), overwrite=True)
    # Read the shards back the SAME way the consumers do (dbutils.fs.head) and assert
    # the payload round-trips byte-for-byte. If head ever truncates a shard, this
    # fails LOUD right here -- the caller catches it, marks the handoff degraded, and
    # the consumers recover from the durable log -- instead of silently handing a
    # corrupted/short list downstream.
    readback = "".join(
        dbutils.fs.head(f"{d}/part-{idx:05d}.json", 1 << 30) for idx in range(len(chunks))
    )
    if readback != payload:
        raise RuntimeError(
            f"handoff self-check failed for {d}: wrote {len(payload)} chars but read "
            f"back {len(readback)} -- a shard was truncated on read. Lower "
            f"_HANDOFF_CHUNK_CHARS or change the handoff store."
        )
    return d

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

# Simulate-only stress knob: in simulate mode, append this many synthetic HARD
# failures (each with a ~1 KB error) so the cross-task handoff can be proven to
# never overflow the task-value cap -- the exact failure customers hit at scale.
dbutils.widgets.text("simulate_hard_count", "0", "Simulate: synthetic hard failures")
try:
    SIM_HARD = int((dbutils.widgets.get("simulate_hard_count") or "0").strip() or "0")
except ValueError:
    SIM_HARD = 0

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
        if name.rsplit(".", 1)[-1].startswith("sim_hard_"):
            # synthetic non-OOM failure with a long ~1 KB message, mimicking the
            # row-level-delete errors that fail many tables at once.
            raise RuntimeError(
                "AnalysisException: foreign Iceberg table uses row-level deletes "
                "(merge-on-read), unsupported for " + name + " :: " + ("detail " * 150)
            )
        return  # treated as a clean serverless success
    spark.sql(f"REFRESH FOREIGN TABLE {name}")

# COMMAND ----------

# Live runs the real scope list; simulate runs the canned sample tables (so it
# works and exercises both branches even without the real scope_tables.py).
if MODE == "simulate":
    tables_to_process = list(SIMULATED_ERRORS.keys()) + [
        "catalog.schema.clean_table_1",
        "catalog.schema.clean_table_2",
    ] + [f"catalog.schema.sim_hard_{i}" for i in range(SIM_HARD)]
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

# Publish the two COUNTS the condition tasks branch on RIGHT NOW -- before the
# durable-log write and the DBFS handoff -- so even if a later line ever throws,
# check_oom_failures / check_hard_failures still route correctly. Bounded ints;
# nothing here can approach the task-value cap.
dbutils.jobs.taskValues.set("failed_count", len(retry_on_classic))
dbutils.jobs.taskValues.set("hard_failure_count", len(hard_failures))

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

# MAGIC %md
# MAGIC ## Hand off to the downstream tasks
# MAGIC
# MAGIC The per-table lists go to DBFS (sharded JSON, addressed by run_id). Task
# MAGIC values carry only fixed-size scalars: the two counts that the condition
# MAGIC tasks compare, and the DBFS handoff directory the consumer tasks read. None
# MAGIC of these grows with the number of failed tables, so "task value is too
# MAGIC large" can never happen - at 1, 200, or 100k tables.
# MAGIC
# MAGIC The whole handoff is wrapped so a DBFS hiccup cannot kill the run. If a
# MAGIC write somehow fails, we still publish the counts the conditions need and
# MAGIC mark the handoff degraded; the consumers treat a missing/unreadable file as
# MAGIC an empty list AND re-derive completeness from the durable log when one is
# MAGIC configured (see 02_fallback_classic).

# COMMAND ----------

# Write the lists to DBFS first; the returned dir is what the consumers read.
# Default the published paths to "" so that, even if a write throws, we still
# set every task value the condition tasks and consumers look for.
oom_handoff_dir = ""
hard_handoff_dir = ""
handoff_ok = True
try:
    oom_handoff_dir = write_handoff(RUN_ID, "oom_tables", retry_on_classic)
    hard_handoff_dir = write_handoff(RUN_ID, "hard_failures", hard_failures)
    print(f"OOM retry list   -> {oom_handoff_dir} ({len(retry_on_classic)} table(s))")
    print(f"hard-failure list -> {hard_handoff_dir} ({len(hard_failures)} table(s))")
except Exception as e:  # noqa: BLE001 -- the handoff must never fail the run
    handoff_ok = False
    print(f"WARN: DBFS handoff write failed: {type(e).__name__}: {e}")

# Bounded task values only: short path strings + a status flag + the log/run id.
# Their size is independent of how many tables failed. (The two counts the
# condition tasks read were already published above, right after classification.)
dbutils.jobs.taskValues.set("oom_handoff_dir", oom_handoff_dir)
dbutils.jobs.taskValues.set("hard_handoff_dir", hard_handoff_dir)
dbutils.jobs.taskValues.set("handoff_ok", handoff_ok)
# Also publish the log table + run id so the classic fallback can re-derive the
# complete OOM list from the durable log if the DBFS handoff is ever unreadable.
dbutils.jobs.taskValues.set("failure_log_table", LOG_TABLE)
dbutils.jobs.taskValues.set("run_id", RUN_ID)
