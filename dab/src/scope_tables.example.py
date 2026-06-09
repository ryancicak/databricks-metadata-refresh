# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh scope: the static table list (TEMPLATE)
# MAGIC
# MAGIC **This is a committed placeholder. The real list is never committed.**
# MAGIC
# MAGIC ## First-time setup
# MAGIC Copy this file to `scope_tables.py` in the same folder and replace the
# MAGIC placeholder entries with your real fully-qualified table names:
# MAGIC
# MAGIC ```bash
# MAGIC cp src/scope_tables.example.py src/scope_tables.py
# MAGIC # then edit src/scope_tables.py
# MAGIC ```
# MAGIC
# MAGIC `src/scope_tables.py` is `.gitignore`d so the table inventory stays out of
# MAGIC version control, but it is force-included in the bundle `sync` block so it
# MAGIC still deploys to the workspace. `01_try_serverless` loads it with
# MAGIC `%run ./scope_tables`.
# MAGIC
# MAGIC ## How the real list is selected
# MAGIC Tables worth proactively refreshing: high object count AND high access in
# MAGIC the last 90 days. Lower the access threshold to widen scope.

# COMMAND ----------

# Fully qualified names: <catalog>.<schema>.<table>
# Replace these placeholders with the real scope list in scope_tables.py.
SCOPE_TABLES = [
    "catalog.schema.table_a",
    "catalog.schema.table_b",
    "catalog.schema.table_c",
]

# COMMAND ----------

print(f"scope_tables: {len(SCOPE_TABLES)} tables loaded")
assert len(SCOPE_TABLES) == len(set(SCOPE_TABLES)), "duplicate table in SCOPE_TABLES"
