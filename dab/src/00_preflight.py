# Databricks notebook source
# MAGIC %md
# MAGIC # 0. Preflight
# MAGIC
# MAGIC Runs first, on serverless, and fails the whole run with a plain-English
# MAGIC message if the bundle is misconfigured, so you find out before any table
# MAGIC is touched or any cluster is started. This is what stops a misconfigured
# MAGIC run from either dying with a cryptic error later or reporting success
# MAGIC while doing nothing useful.
# MAGIC
# MAGIC Blocking checks:
# MAGIC  1. the scope list loaded and is not empty
# MAGIC  2. failure_log_table is blank (disabled) or a real catalog.schema that exists
# MAGIC  3. the classic worker and driver instance types exist in this workspace
# MAGIC  4. the cross-task handoff store (DBFS) round-trips here; if it cannot AND
# MAGIC     failure_log_table is blank, the classic fallback has no channel for its list
# MAGIC
# MAGIC What it cannot check from here (no AWS access): on-demand quota and free
# MAGIC subnet IPs for the classic cluster. Those still have to be confirmed in
# MAGIC your own account. Whether each scope table is reachable is left to the
# MAGIC serverless pass, which classifies a bad name as a hard failure anyway.

# COMMAND ----------

# MAGIC %run ./scope_tables

# COMMAND ----------

dbutils.widgets.text("failure_log_table", "", "Failure log table (catalog.schema.table)")
dbutils.widgets.text("classic_node_type", "", "Classic worker instance type")
dbutils.widgets.text("classic_driver_type", "", "Classic driver instance type")
LOG_TABLE = dbutils.widgets.get("failure_log_table").strip()
WORKER_TYPE = dbutils.widgets.get("classic_node_type").strip()
DRIVER_TYPE = dbutils.widgets.get("classic_driver_type").strip()

problems = []   # any of these blocks the run
warnings = []   # surfaced, but the run continues

# COMMAND ----------

# Check 1: the scope list is present and not empty.
try:
    scope = list(SCOPE_TABLES)
except NameError:
    scope = []
    problems.append(
        "scope_tables.py did not load (SCOPE_TABLES is undefined). Copy "
        "src/scope_tables.example.py to src/scope_tables.py and fill in your tables."
    )
if not scope:
    problems.append("SCOPE_TABLES is empty. Add at least one catalog.schema.table to refresh.")
else:
    print(f"scope list: {len(scope)} table(s)  ok")

# COMMAND ----------

# Check 2: failure_log_table is usable. Blank means the durable log is off,
# which is allowed. A leftover placeholder or a missing schema is not.
if not LOG_TABLE:
    print("failure_log_table: blank, durable log disabled  ok")
elif "<" in LOG_TABLE or ">" in LOG_TABLE:
    problems.append(
        f"failure_log_table is still the placeholder '{LOG_TABLE}'. Set it to a "
        "catalog.schema.table you can write to, or set it to an empty string to disable the log."
    )
else:
    parts = LOG_TABLE.split(".")
    if len(parts) != 3:
        problems.append(
            f"failure_log_table '{LOG_TABLE}' is not a three-part catalog.schema.table name."
        )
    else:
        cat, sch, _ = parts
        try:
            spark.sql(f"DESCRIBE SCHEMA `{cat}`.`{sch}`")
            print(f"failure_log_table target schema {cat}.{sch} exists  ok")
        except Exception as e:  # noqa: BLE001
            problems.append(
                f"failure_log_table is set to '{LOG_TABLE}' but schema {cat}.{sch} does not "
                f"exist or is not visible ({type(e).__name__}). Create it, or blank the variable."
            )

# COMMAND ----------

# Check 3: the classic instance types exist in this workspace. This catches a
# "node type not supported" before the fallback tries (and fails) to launch.
# It does NOT prove on-demand quota or free subnet IPs are available; confirm
# those in your AWS account separately.
try:
    from databricks.sdk import WorkspaceClient

    available = {nt.node_type_id for nt in WorkspaceClient().clusters.list_node_types().node_types}
    for label, t in (("classic_node_type", WORKER_TYPE), ("classic_driver_type", DRIVER_TYPE)):
        if not t:
            continue
        if t in available:
            print(f"{label} {t}: available  ok")
        else:
            problems.append(
                f"{label} '{t}' is not available in this workspace. Pick a type this workspace "
                "allows (Compute > Create cluster shows the list) and set the variable."
            )
except Exception as e:  # noqa: BLE001 -- never let the check itself break the run
    warnings.append(
        f"could not verify classic instance types ({type(e).__name__}): {str(e)[:160]}. "
        "Confirm the types are available in this workspace manually."
    )

# COMMAND ----------

# Check 4: the cross-task handoff store works on THIS compute. The producer hands
# the (possibly large) failure lists to the classic + logging tasks via a per-run
# DBFS file (dbutils.fs), because task values are size-capped. On a UC-only /
# DBFS-root-disabled workspace that write can be blocked. Prove it now with a
# write+read round-trip through the SAME head() path the consumers use, at a
# realistic ~49 KB shard (under head's 64 KiB read cap), so a non-functional store
# fails fast here instead of mid-run.
import json as _json

_canary = "dbfs:/tmp/metadata_refresh/preflight_canary"
try:
    _payload = _json.dumps({"canary": "x" * 49000})
    try:
        dbutils.fs.rm(_canary, recurse=True)
    except Exception:  # noqa: BLE001
        pass
    dbutils.fs.mkdirs(_canary)
    dbutils.fs.put(f"{_canary}/part-00000.json", _payload, overwrite=True)
    _back = dbutils.fs.head(f"{_canary}/part-00000.json", 1 << 30)
    try:
        dbutils.fs.rm(_canary, recurse=True)
    except Exception:  # noqa: BLE001
        pass
    if _back != _payload:
        raise RuntimeError("DBFS write+read round-trip did not match (read truncated?)")
    print("handoff store (DBFS): write+read round-trip ok")
except Exception as e:  # noqa: BLE001
    _have_log = bool(LOG_TABLE) and "<" not in LOG_TABLE and ">" not in LOG_TABLE
    if _have_log:
        warnings.append(
            f"DBFS handoff store is not usable here ({type(e).__name__}: {str(e)[:120]}). "
            "The run will rely on failure_log_table to hand off the OOM-retry list, and "
            "hard-failure detail may show as a count only. Functional but degraded."
        )
    else:
        problems.append(
            f"DBFS handoff store is not usable here ({type(e).__name__}: {str(e)[:120]}) AND "
            "failure_log_table is blank. The classic OOM fallback would have no channel to "
            "receive its table list. Set failure_log_table to a catalog.schema.table you can "
            "write, then re-run."
        )

# COMMAND ----------

for w in warnings:
    print(f"WARNING: {w}")

if problems:
    msg = "Preflight failed. Fix these before running:\n" + "\n".join(
        f"  {i}. {p}" for i, p in enumerate(problems, 1)
    )
    raise RuntimeError(msg)

print("\nPreflight passed. Safe to refresh.")
