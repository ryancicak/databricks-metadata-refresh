"""
oom_demo.py — build a foreign (Glue) Iceberg table on EMR whose Iceberg
metadata is pathologically large, to reproduce the Databricks "convert the
table -> OutOfMemoryError" failure on REFRESH FOREIGN TABLE.

Runs on EMR as a spark-submit Step. The Glue Iceberg catalog ("glue_catalog")
is supplied at submit time via --conf (see run_step.sh), so this never touches
the cluster's default UC IRC catalog.

Modes (argv[1]):
  hello    - tiny 3-row table (pipeline de-risk)
  big      - the pathological wide-table / huge-manifest build
  register - re-register an EXISTING Iceberg table (data already in S3) into Glue
             from its metadata.json pointer; no rebuild, no data touched
  inspect  - print metadata stats (files, manifest count, manifest sizes) for a table
  drop     - drop a table

Usage on EMR:
  spark-submit ... oom_demo.py hello
  spark-submit ... oom_demo.py big   --tbl oom_big --cols 1000 --files 20000 --commits 1
  spark-submit ... oom_demo.py inspect --tbl oom_big
"""
import sys

from pyspark.sql import SparkSession

CAT = "glue_catalog"
DB = "oom_demo"


def get_arg(flag, default=None, cast=str):
    if flag in sys.argv:
        return cast(sys.argv[sys.argv.index(flag) + 1])
    return default


def spark_session():
    s = SparkSession.builder.appName("oom_demo").getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    return s


def full(tbl):
    return f"{CAT}.{DB}.{tbl}"


def show_meta(spark, tbl):
    f = full(tbl)
    nfiles = spark.sql(f"SELECT count(*) c FROM {f}.files").first().c
    nman = spark.sql(f"SELECT count(*) c FROM {f}.manifests").first().c
    print(f"\n=== METADATA for {f} ===")
    print(f"  data files       : {nfiles}")
    print(f"  manifest files   : {nman}")
    rows = spark.sql(
        f"SELECT path, length FROM {f}.manifests ORDER BY length DESC"
    ).take(8)
    total = spark.sql(f"SELECT sum(length) s FROM {f}.manifests").first().s or 0
    print(f"  total manifest bytes : {total:,} ({total/1024/1024:.1f} MB)")
    print("  largest manifests:")
    for r in rows:
        print(f"     {r['length']:>14,} bytes  ({r['length']/1024/1024:7.2f} MB)  {r['path']}")
    snap = spark.sql(
        f"SELECT snapshot_id, manifest_list FROM {f}.snapshots ORDER BY committed_at DESC"
    ).first()
    if snap:
        print(f"  current manifest_list : {snap['manifest_list']}")


def main():
    global CAT, DB
    mode = sys.argv[1] if len(sys.argv) > 1 else "hello"
    # --catalog/--schema let us target either the Glue catalog (default) or the
    # cluster's UC Iceberg-REST catalog (your_catalog) for the no-LF path.
    CAT = get_arg("--catalog", CAT)
    DB = get_arg("--schema", DB)
    spark = spark_session()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CAT}.{DB}")
    print(f"namespace ready: {CAT}.{DB}")

    if mode == "hello":
        t = "hello"
        spark.sql(f"DROP TABLE IF EXISTS {full(t)}")
        spark.sql(f"CREATE TABLE {full(t)} (id bigint, name string) USING iceberg")
        spark.sql(f"INSERT INTO {full(t)} VALUES (1,'alice'),(2,'bob'),(3,'carol')")
        print("HELLO table created + populated.")
        spark.table(full(t)).show()
        show_meta(spark, t)

    elif mode == "inspect":
        show_meta(spark, get_arg("--tbl", "hello"))

    elif mode == "drop":
        t = get_arg("--tbl")
        spark.sql(f"DROP TABLE IF EXISTS {full(t)}")
        print(f"dropped {full(t)}")

    elif mode == "register":
        # Re-attach an Iceberg table whose DATA + METADATA already live in S3 but
        # whose Glue catalog entry was lost (e.g. old workspace torn down). Uses
        # Iceberg's register_table procedure: it only reads the metadata.json
        # pointer and writes the Glue entry -- it does NOT rewrite or purge data.
        t = get_arg("--tbl")
        metadata_file = get_arg("--metadata")
        if not t or not metadata_file:
            print("register needs --tbl <name> --metadata s3://.../xxxxx.metadata.json")
            sys.exit(2)
        spark.sql(
            f"CALL {CAT}.system.register_table("
            f"table => '{DB}.{t}', metadata_file => '{metadata_file}')"
        )
        print(f"registered {full(t)} from {metadata_file}")
        # light verify: manifest count + total bytes reads only the manifest LIST,
        # not the (multi-GB) manifests themselves -- safe/cheap even for oom_big.
        row = spark.sql(
            f"SELECT count(*) c, coalesce(sum(length),0) b FROM {full(t)}.manifests"
        ).first()
        print(f"  manifests={row.c}  total_manifest_bytes={row.b:,} ({row.b/1024/1024:.1f} MB)")

    elif mode == "big":
        # Pathological table: a WIDE table (many columns, all carrying truncated
        # min/max bounds) x many tiny data files, packed into ONE manifest. The
        # manifest size ~ (#files x #columns-with-stats), so Databricks OOMs when
        # it deserializes the whole manifest to convert the table on
        # REFRESH FOREIGN TABLE. No big DATA needed -- it's all metadata.
        t = get_arg("--tbl", "oom_big")
        cols = get_arg("--cols", 500, int)
        files = get_arg("--files", 3000, int)
        metrics = get_arg("--metrics", "full")   # full = untruncated bounds (fat)
        vlen = get_arg("--vlen", 256, int)        # per-value char length; high-entropy
        appends = get_arg("--appends", 1, int)    # number of append snapshots (=> manifests)
        print(f"BIG build: tbl={t} cols={cols} files={files} appends={appends} metrics={metrics} vlen={vlen}")
        spark.sql(f"DROP TABLE IF EXISTS {full(t)}")

        # Each column value = high-entropy string of length vlen (concatenated
        # sha2 chunks). With metrics=full the manifest stores the FULL min/max
        # bound per column per file -> with 1 row/file lower==upper==that value,
        # so each entry carries ~cols*vlen bytes of incompressible bound data.
        def colexpr(k):
            if vlen > 0:
                nch = vlen // 64 + 1
                chunks = ", ".join(
                    f"sha2(concat(cast(id as string),'_{k}_{j}'),256)" for j in range(nch)
                )
                return f"substr(concat({chunks}), 1, {vlen}) AS c{k}"
            return f"rpad(concat('{k}_', cast(id as string)), 18, 'x') AS c{k}"

        def make_df(start):
            exprs = ["id"] + [colexpr(k) for k in range(cols)]
            # round-robin repartition(files) => ~1 row/partition => 1 data file each.
            return spark.range(start, start + files).selectExpr(*exprs).repartition(files)

        for i in range(appends):
            df = make_df(i * files)
            if i == 0:
                (
                    df.writeTo(full(t))
                    .using("iceberg")
                    .tableProperty("write.metadata.metrics.default", metrics)
                    .tableProperty("write.metadata.metrics.max-inferred-column-defaults", str(cols + 10))
                    # each append rolls its own manifest(s); merge off => they accumulate:
                    .tableProperty("commit.manifest.target-size-bytes", "2147483648")
                    .tableProperty("commit.manifest-merge.enabled", "false")
                    .createOrReplace()
                )
            else:
                df.writeTo(full(t)).append()
            print(f"  append {i + 1}/{appends} done ({files} files)")
        print(f"wrote {full(t)} ({appends} appends x {files} files x {cols} cols, metrics={metrics}, vlen={vlen})")
        show_meta(spark, t)

    elif mode == "append":
        # Append `files` NEW files to an EXISTING table (no drop). Used to test how
        # Databricks materializes a new Iceberg snapshot on REFRESH FOREIGN TABLE:
        # a full re-CLONE (numCopiedFiles == total) vs incremental (only new files).
        t = get_arg("--tbl", "oom_cal")
        cols = get_arg("--cols", 500, int)
        files = get_arg("--files", 50, int)
        vlen = get_arg("--vlen", 256, int)
        start = get_arg("--start", 10_000_000, int)  # high id offset so files are new
        print(f"APPEND: tbl={t} files={files} cols={cols} vlen={vlen} start={start}")

        def colexpr(k):
            if vlen > 0:
                nch = vlen // 64 + 1
                chunks = ", ".join(
                    f"sha2(concat(cast(id as string),'_{k}_{j}'),256)" for j in range(nch)
                )
                return f"substr(concat({chunks}), 1, {vlen}) AS c{k}"
            return f"rpad(concat('{k}_', cast(id as string)), 18, 'x') AS c{k}"

        exprs = ["id"] + [colexpr(k) for k in range(cols)]
        df = spark.range(start, start + files).selectExpr(*exprs).repartition(files)
        df.writeTo(full(t)).append()
        print(f"appended {files} new files to {full(t)}")
        show_meta(spark, t)

    else:
        print(f"unknown mode: {mode}")
        sys.exit(2)

    print("\nDONE.")


if __name__ == "__main__":
    main()
