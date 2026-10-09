"""03_Silver: current version per record_id via deterministic dedup + Delta MERGE.

Priority when several observations of the same record_id exist (in one batch
or across runs): newest source_datestamp, then newest fetched_at, then the
greatest raw_file_path, then the greatest record_index. An observation older
than what Silver already holds (source_datestamp < target) is ignored.

Actions (computed before the MERGE, from the same target snapshot):
  new             record_id not in Silver, not deleted        -> INSERT
  changed         content_hash differs                         -> UPDATE
  restored        Silver row is deleted, observation is not    -> UPDATE
  deleted         deleted header for an active Silver row      -> UPDATE is_deleted
  unchanged       same content and deletion status             -> untouched
  stale_ignored   observation older than Silver's datestamp    -> untouched
  deleted_unknown deleted header for an id never seen          -> untouched
"""

from __future__ import annotations

from .cleaning import build_silver_candidate
from .config import PipelineConfig, iso_utc, utc_now

BUSINESS_COLUMNS = (
    "title", "titles", "abstract", "abstracts", "subjects", "publication_year", "publication_year_status",
    "date_raw", "language", "language_codes", "language_raw", "publishers", "university", "university_group",
    "university_method", "university_candidates", "document_types", "dc_identifiers", "source_url", "set_specs",
)
APPLIED_ACTIONS = ("new", "changed", "restored", "deleted")
ALL_ACTIONS = ("new", "changed", "restored", "deleted", "unchanged", "stale_ignored", "deleted_unknown")


def _business_fields():
    from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField

    arr = ArrayType(StringType(), containsNull=False)
    types = {
        "title": StringType(), "titles": arr, "abstract": StringType(), "abstracts": arr, "subjects": arr,
        "publication_year": IntegerType(), "publication_year_status": StringType(), "date_raw": arr,
        "language": StringType(), "language_codes": arr, "language_raw": arr, "publishers": arr,
        "university": StringType(), "university_group": StringType(), "university_method": StringType(),
        "university_candidates": arr, "document_types": arr, "dc_identifiers": arr, "source_url": StringType(),
        "set_specs": arr,
    }
    nullable = {"title", "abstract", "publication_year", "university", "source_url"}
    return [StructField(c, types[c], c in nullable) for c in BUSINESS_COLUMNS]


def silver_schema():
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

    return StructType(
        [StructField("record_id", StringType(), False)]
        + _business_fields()
        + [
            StructField("content_hash", StringType(), False),
            StructField("is_deleted", BooleanType(), False),
            StructField("source_datestamp", TimestampType(), False),
            StructField("source_observation_id", StringType(), False),
            StructField("source_raw_file_path", StringType(), False),
            StructField("source_raw_file_sha256", StringType(), False),
            StructField("source_run_id", StringType(), False),
            StructField("first_seen_at", TimestampType(), False),
            StructField("first_seen_run_id", StringType(), False),
            StructField("last_changed_at", TimestampType(), False),
            StructField("last_changed_run_id", StringType(), False),
        ]
    )


def candidate_schema():
    from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType, TimestampType

    return StructType(
        [StructField("record_id", StringType(), False)]
        + _business_fields()
        + [
            StructField("content_hash", StringType(), False),
            StructField("deleted", BooleanType(), False),
            StructField("source_datestamp", TimestampType(), False),
            StructField("fetched_at", TimestampType(), False),
            StructField("observation_id", StringType(), False),
            StructField("raw_file_path", StringType(), False),
            StructField("raw_file_sha256", StringType(), False),
            StructField("record_index", IntegerType(), False),
            StructField("run_id", StringType(), False),
        ]
    )


def to_candidates(spark, bronze_df):
    """Bronze observations (selected or deleted) -> cleaned candidate rows with content_hash.

    Cleaning runs in plain Python on the driver: input volume is capped by
    max_records per run, and the rules stay unit-testable without Spark.
    """
    rows = []
    for r in bronze_df.where("is_selected").collect():
        obs = r.asDict(recursive=True)
        c = build_silver_candidate(obs)
        c.update({
            "deleted": obs["deleted"], "source_datestamp": obs["source_datestamp"], "fetched_at": obs["fetched_at"],
            "observation_id": obs["observation_id"], "raw_file_path": obs["raw_file_path"],
            "raw_file_sha256": obs["raw_file_sha256"], "record_index": obs["record_index"], "run_id": obs["run_id"],
        })
        rows.append(c)
    return spark.createDataFrame(rows, candidate_schema())


def deduplicate(candidates_df):
    """Keep exactly one (the highest-priority) observation per record_id."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("record_id").orderBy(
        F.col("source_datestamp").desc(), F.col("fetched_at").desc(),
        F.col("raw_file_path").desc(), F.col("record_index").desc())
    return candidates_df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def classify(spark, cfg: PipelineConfig, batch_df):
    from pyspark.sql import functions as F

    target = spark.table(cfg.tables["silver"]).select(
        "record_id", F.col("content_hash").alias("t_hash"), F.col("is_deleted").alias("t_deleted"),
        F.col("source_datestamp").alias("t_ds"))
    j = batch_df.join(target, "record_id", "left")
    action = (
        F.when(F.col("t_ds").isNull() & F.col("deleted"), "deleted_unknown")
        .when(F.col("t_ds").isNull(), "new")
        .when(F.col("source_datestamp") < F.col("t_ds"), "stale_ignored")
        .when(F.col("deleted") & F.col("t_deleted"), "unchanged")
        .when(F.col("deleted"), "deleted")
        .when(F.col("t_deleted"), "restored")
        .when(F.col("content_hash") != F.col("t_hash"), "changed")
        .otherwise("unchanged")
    )
    return j.withColumn("_action", action).drop("t_hash", "t_deleted", "t_ds")


def merge_into_silver(spark, cfg: PipelineConfig, classified_df) -> None:
    from delta.tables import DeltaTable

    src = classified_df.where(classified_df["_action"].isin(*APPLIED_ACTIONS))
    trace = {
        "source_datestamp": "s.source_datestamp",
        "source_observation_id": "s.observation_id",
        "source_raw_file_path": "s.raw_file_path",
        "source_raw_file_sha256": "s.raw_file_sha256",
        "source_run_id": "s.run_id",
        "last_changed_at": "s.fetched_at",
        "last_changed_run_id": "s.run_id",
    }
    content = {c: f"s.{c}" for c in BUSINESS_COLUMNS}
    content.update({"content_hash": "s.content_hash", "is_deleted": "false"})
    insert = {"record_id": "s.record_id", **content, **trace,
              "first_seen_at": "s.fetched_at", "first_seen_run_id": "s.run_id"}
    # Guard in the MERGE itself as well: never let an older observation win.
    newer = "s.source_datestamp >= t.source_datestamp"
    (
        DeltaTable.forName(spark, cfg.tables["silver"]).alias("t")
        .merge(src.alias("s"), "t.record_id = s.record_id")
        .whenMatchedUpdate(condition=f"s._action = 'deleted' AND {newer}",
                           set={"is_deleted": "true", **trace})
        .whenMatchedUpdate(condition=f"s._action IN ('changed', 'restored') AND {newer}",
                           set={**content, **trace})
        .whenNotMatchedInsert(condition="s._action = 'new'", values=insert)
        .execute()
    )


def run_silver(spark, cfg: PipelineConfig, run_id: str) -> dict:
    from pyspark.sql import functions as F

    from .spark_utils import ensure_table, last_operation_metrics

    t = cfg.tables
    ensure_table(spark, t["silver"], silver_schema())
    bronze = spark.table(t["bronze"]).where(F.col("run_id") == run_id)
    candidates = to_candidates(spark, bronze)
    n_in = candidates.count()
    batch = deduplicate(candidates)
    n_batch = batch.count()
    classified = classify(spark, cfg, batch).cache()
    counts = {a: 0 for a in ALL_ACTIONS}
    for r in classified.groupBy("_action").count().collect():
        counts[r["_action"]] = r["count"]
    merge_metrics = {}
    if sum(counts[a] for a in APPLIED_ACTIONS):
        merge_into_silver(spark, cfg, classified)
        merge_metrics = last_operation_metrics(spark, t["silver"])
    classified.unpersist()
    return {
        "run_id": run_id,
        "input_observations": n_in,
        "records_in_batch_after_dedup": n_batch,
        "duplicates_removed_in_batch": n_in - n_batch,
        "actions": counts,
        "delta_merge_metrics": merge_metrics,
        "finished_at_utc": iso_utc(utc_now()),
    }
