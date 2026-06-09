# How to Monitor Foreign-Catalog Metadata Refreshes

See when your foreign (Glue / Iceberg) tables refresh — and catch the ones that run
**out of memory** — without checking every table one by one.

---

## ✅ Start here: the one query

Shows **every** `REFRESH FOREIGN TABLE`, across **all** tables, newest first:

```sql
SELECT start_time, statement_text, execution_status, total_duration_ms, error_message
FROM system.query.history
WHERE statement_text ILIKE 'REFRESH FOREIGN TABLE%'
ORDER BY start_time DESC;
```

### How to read it

| What you see | What it means |
|---|---|
| `execution_status = FINISHED` | ✅ refresh worked |
| `FAILED` + `TASK_FAILED_EXECUTOR_LOSS` / `oom` / `GC overhead` in `error_message` | 💥 **out of memory** — retry on a bigger cluster |
| `FAILED` + `TABLE_OR_VIEW_NOT_FOUND` / permission error | ⚠️ name or access issue (not memory) |
| big `total_duration_ms` | heavy = **full** refresh; small = **incremental** |

---

## 🕒 "When did each table last refresh, and who ran it?"

```sql
SELECT table_name, last_altered, last_altered_by
FROM system.information_schema.tables
WHERE table_catalog = '<your_foreign_catalog>'
ORDER BY last_altered DESC;
```

---

## 🔍 Only if you need the exact full-vs-incremental detail

This is the **one** thing that needs a per-table look:

```sql
DESCRIBE HISTORY <your_foreign_catalog>.<schema>.<table>;
```

Look at the `CLONE` row's `operationMetrics`:

- `numCopiedFiles` **=** `sourceNumOfFiles` → **FULL** refresh (re-reads everything — the heavy, OOM-prone one)
- `numCopiedFiles` **≪** `sourceNumOfFiles` → **INCREMENTAL** (only the new files — cheap)

---

## 🧭 TL;DR — pick by your question

| Your question | Where to look |
|---|---|
| Which refreshes ran / failed / **OOM'd**? | `system.query.history` |
| When / who last refreshed each table? | `system.information_schema.tables` |
| Exact full-vs-incremental file counts? | `DESCRIBE HISTORY <table>` |

> **Rule of thumb:** use `system.query.history` for day-to-day monitoring (it catches the OOMs).
> Only reach for `DESCRIBE HISTORY` when you need the precise copied-file numbers on one table.

<sub>(Also available: `system.access.audit` logs the underlying Unity Catalog operations
`updateMetadataSnapshot` and `commitDeltaUniformMetadata` — useful for a who-did-what audit trail.)</sub>
