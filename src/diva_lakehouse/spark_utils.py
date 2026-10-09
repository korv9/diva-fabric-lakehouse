"""Spark / Delta helpers shared by the Bronze, Silver and Gold stages.

In Fabric the notebook already provides a `spark` session with Delta Lake
configured; get_spark() just returns it. Locally a Delta-enabled session is
created with delta-spark (tests and scripts/run_local_demo.py only).
"""

from __future__ import annotations


def get_spark(local_warehouse: str | None = None, app_name: str = "diva-lakehouse"):
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if local_warehouse is None:
        return SparkSession.builder.getOrCreate()
    import os
    import sys

    from delta import configure_spark_with_delta_pip  # local only

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", local_warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={local_warehouse}/_derby")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def table_exists(spark, name: str) -> bool:
    return bool(spark.catalog.tableExists(name))


def ensure_table(spark, name: str, schema) -> None:
    """Create an empty Delta table with an explicit schema if it does not exist."""
    if not table_exists(spark, name):
        spark.createDataFrame([], schema).write.format("delta").saveAsTable(name)


def merge_insert_only(spark, name: str, df, key: str) -> int:
    """Insert rows whose key is not yet present (idempotent append).

    Returns the number of inserted rows (row count after - before), which is
    also correct when the MERGE inserts nothing.
    """
    from delta.tables import DeltaTable

    ensure_table(spark, name, df.schema)
    before = spark.table(name).count()
    (
        DeltaTable.forName(spark, name).alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return spark.table(name).count() - before


def last_operation_metrics(spark, name: str) -> dict:
    from delta.tables import DeltaTable

    row = DeltaTable.forName(spark, name).history(1).select("operation", "operationMetrics", "version").first()
    metrics = dict(row["operationMetrics"] or {})
    return {"operation": row["operation"], "version": row["version"],
            **{k: int(v) for k, v in metrics.items() if str(v).isdigit()}}


def overwrite_table(df, name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)


def drop_table(spark, name: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {name}")
