# Foreign Catalog Metadata Refresh — Databricks Asset Bundle

Refreshes the in-scope foreign-catalog tables on **serverless first**. Any table
that fails with an out-of-memory error is retried on a **classic cluster** that
carries the static `spark.task.cpus=8` conf those tables need. Non-OOM failures
(bad name, permissions, schema) are logged and alerted, never retried on classic.

![Refresh pattern](docs/refresh_pattern.png)

---

## 1. What you must replace

| Where | Replace |
|---|---|
| `databricks.yml` → `targets.dev` / `targets.prod` / `targets.test` → `host` | your workspace URL(s) — every host is a `<...>  # REPLACE` placeholder |
| `src/scope_tables.py` | the table list. Copy the template and fill it in: `cp src/scope_tables.example.py src/scope_tables.py`, then paste your fully-qualified table names. (This file is gitignored — it never gets committed — but still deploys.) |

Optional, in `databricks.yml` → `variables`:
- `failure_log_table` — **off by default (blank).** When off, failures are still
  logged to each task's run output. Set it to a `<catalog>.<schema>.<table>` only
  if you want a durable, queryable record — it's a UC **managed** Delta table, so
  you just need `CREATE TABLE` on a schema the job's run-as principal owns (no S3
  path, volume, or storage location to configure).
- `classic_num_workers` / `classic_node_type` / `classic_driver_type` — classic
  cluster size (defaults: 80 × `r6gd.2xlarge`, driver `r6gd.8xlarge`).

---

## 2. Test it (simulate)

Simulate replays real OOM and hard-failure errors against sample tables, so it
exercises the whole flow — including spinning up the classic cluster — without
touching any real table. The `test` target uses a 1-node classic cluster so it
costs pennies.

```bash
databricks bundle deploy -t test -p <your-profile>
databricks bundle run metadata_refresh -t test -p <your-profile> --notebook-params mode=simulate
```

Expected: the run succeeds. `try_serverless` classifies the sample errors,
`fallback_classic` retries the OOM ones on the classic cluster, and
`log_hard_failures` logs the one simulated non-OOM failure (without failing the
run). Check the `fallback_classic` output for `spark.task.cpus = 8`.

Tear the test job down when done:

```bash
databricks bundle destroy -t test -p <your-profile>
```

---

## 3. Go live

```bash
databricks bundle deploy -t dev -p <your-profile>
databricks bundle run   metadata_refresh -t dev -p <your-profile> --notebook-params mode=live
```

The job is created **paused**. Once a live run looks right, unpause it in the
Jobs UI (or set `pause_status: UNPAUSED` in `resources/metadata_refresh.job.yml`).
Default schedule is the top of every 4th hour (`schedule_cron` in `databricks.yml`).

Deploy `-t prod` for the production target.

---

## How tasks hand off the failure lists

The tasks run on different compute (serverless for the passes, a classic cluster
for the fallback), so they can only share data through `dbutils.jobs.taskValues`
(small, hard size cap) or a store both can reach. The per-table lists — the OOM
tables to retry and the hard failures to log — can be most of the scope at once
(foreign-Iceberg federation cannot read merge-on-read / row-level-delete tables,
so a big chunk fails together), which overflowed the task-value cap and failed
the whole run on the handoff line (`INVALID_PARAMETER_VALUE: The task value is
too large`).

The lists now go to a **per-run JSON file on DBFS** (`dbfs:/tmp/metadata_refresh
/handoff/<run_id>/...`, sharded so no single write exceeds the `dbutils.fs.put`
limit), and task values carry only fixed-size scalars — the two failure counts
the condition tasks compare, plus the short DBFS path the consumers read. Nothing
on task values grows with the number of failures, so the size error is
structurally impossible at any scale (1, 200, 100k tables). DBFS is used
deliberately: it is the one location reachable from both serverless and classic
with **zero config** (no UC volume, storage location, or `CREATE TABLE` grant),
read/written via `dbutils.fs` rather than the `/dbfs` FUSE mount (FUSE is not
available on serverless). The handoff is wrapped so a DBFS hiccup never fails the
run, and the classic fallback re-derives its complete OOM list from
`failure_log_table` if the file is ever unreadable, then refuses to run a partial
list (it would silently skip real OOM tables).

## Where failures are recorded

The job never fails the run on a refresh error — every table is attempted
independently and errors are recorded, not raised. They show up in two places:

- **Run output (always).** Each failed table prints one truncated line in the
  task output, only when it actually fails (nothing prints per-table on success).
  This needs no write access and no configuration. Large runs are safe — output
  is line-by-line and bounded (no giant single-line dump), so it won't error the
  task however many tables fail.
- **A durable Delta table (optional).** Set `failure_log_table` to turn on a
  queryable record. It's a UC **managed** table, so you only need `CREATE TABLE`
  on a schema the job's run-as principal owns — no S3 path, volume, or storage
  location to set up. Each row is tagged by action: `oom_retry_classic`,
  `hard_skip` (non-OOM, not retried), or `classic_still_failing`. If the write
  ever fails, it warns and the run still succeeds — logging never breaks the job.

---

## Workspace prerequisites

- Serverless **and** classic compute both enabled on the workspace.
- These workspace-level flags set on the target workspace:
  `spark.databricks.delta.uniform.ingress.refreshChecksumValidation.enabled=false`
  and `spark.databricks.delta.autoCompact.recordPartitionStats.enabled=false`.

---

## Files

| path | what it is |
|---|---|
| `databricks.yml` | targets, variables |
| `resources/metadata_refresh.job.yml` | the job + classic cluster |
| `src/scope_tables.example.py` | template for the table list |
| `src/scope_tables.py` | your real table list (gitignored) |
| `src/01_try_serverless.py` | serverless pass, OOM classifier, failure log |
| `src/02_fallback_classic.py` | retries OOM tables on classic |
| `src/03_log_hard_failures.py` | logs non-OOM failures (does not fail the run) |
