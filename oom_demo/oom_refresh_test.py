# Databricks notebook source
# Reproduce the foreign-Iceberg metadata-conversion OOM on SERVERLESS COMPUTE
# (the DAB's path) — REFRESH FOREIGN TABLE deserializes the oversized manifest
# in a task with limited per-task heap. A Photon SQL warehouse hides this.
import time

for tbl in ["your_foreign_catalog.oom_demo.oom_cal", "your_foreign_catalog.oom_demo.oom_big"]:
    t = time.time()
    print(f"==== REFRESH FOREIGN TABLE {tbl} ====")
    spark.sql(f"REFRESH FOREIGN TABLE {tbl}")
    print(f"  REFRESH ok in {round(time.time()-t,1)}s")
    n = spark.table(tbl).count()
    print(f"  count = {n}")
    # force the planner to materialize per-file column stats from the manifest:
    r = spark.sql(f"SELECT count(*) FROM {tbl} WHERE c0 > 'zzzz' AND c499 > 'zzzz'").collect()
    print(f"  filtered scan ok: {r}")

print("DONE — no OOM on this compute")
