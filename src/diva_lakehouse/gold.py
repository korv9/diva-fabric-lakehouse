"""04_Gold: three small aggregates (full rebuild) + JSON/CSV exports.

Grain:
  gold_theses_by_year            one row per publication_year (NULL = unknown/ambiguous year)
  gold_theses_by_university_year one row per (university_group, publication_year). Every active
                                 Silver record is counted exactly once: records matching several
                                 universities go to "Flera lärosäten (ej entydigt)", records
                                 with no identified university to "Okänt lärosäte".
  gold_metadata_completeness     one row per metric over the active Silver records

Deleted records (is_deleted = true) are excluded everywhere. Results describe
only the limited sample harvested by this project.
"""

from __future__ import annotations

import csv
import io

from .config import PipelineConfig, iso_utc, utc_now
from .storage import Lakehouse

LIVE_SCOPE_NOTE = ("Begränsat urval: endast studentuppsatser som detta projekt hämtat via OAI-PMH inom angivna "
                   "datestamp-intervall och max_records. Inte statistik över hela DiVA eller Sveriges uppsatser.")
DEMO_SCOPE_NOTE = "SYNTETISK DEMO-DATA: påhittade poster för att demonstrera pipelinen. Inte verklig statistik."

COMPLETENESS_METRICS = (
    ("title", "title IS NOT NULL"),
    ("abstract", "abstract IS NOT NULL"),
    ("subjects", "size(subjects) > 0"),
    ("publication_year", "publication_year IS NOT NULL"),
    ("identified_university", "university IS NOT NULL"),
)


def scope_note(cfg: PipelineConfig) -> str:
    return DEMO_SCOPE_NOTE if cfg.source_mode == "demo" else LIVE_SCOPE_NOTE


def build_gold(spark, cfg: PipelineConfig, run_id: str) -> dict:
    from pyspark.sql import functions as F

    active = spark.table(cfg.tables["silver"]).where(~F.col("is_deleted"))
    meta = [F.lit(scope_note(cfg)).alias("data_scope"), F.lit(cfg.source_mode).alias("source_mode"),
            F.lit(run_id).alias("built_by_run_id")]

    by_year = (active.groupBy("publication_year").agg(F.count("*").cast("long").alias("thesis_count"))
               .select("publication_year", "thesis_count", *meta).orderBy(F.col("publication_year").asc_nulls_last()))

    by_uni = (active.groupBy("university_group", "publication_year")
              .agg(F.count("*").cast("long").alias("thesis_count"))
              .select("university_group", "publication_year", "thesis_count", *meta)
              .orderBy("university_group", F.col("publication_year").asc_nulls_last()))

    total = active.count()
    rows = []
    for metric, cond in COMPLETENESS_METRICS:
        n = active.where(cond).count()
        rows.append((metric, total, n, round(n / total, 4) if total else None))
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    comp_schema = StructType([StructField("metric", StringType(), False),
                              StructField("records_total", LongType(), False),
                              StructField("records_with_value", LongType(), False),
                              StructField("share", DoubleType(), True)])
    completeness = spark.createDataFrame(rows, comp_schema).select("*", *meta)
    return {"by_year": by_year, "by_university_year": by_uni, "completeness": completeness}


def write_gold(cfg: PipelineConfig, gold: dict) -> None:
    from .spark_utils import overwrite_table

    t = cfg.tables
    overwrite_table(gold["by_year"], t["gold_by_year"])
    overwrite_table(gold["by_university_year"], t["gold_by_university_year"])
    overwrite_table(gold["completeness"], t["gold_completeness"])


def _rows(df) -> list:
    return [r.asDict() for r in df.collect()]


def _csv(rows: list) -> str:
    if not rows:
        return ""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def export_gold(cfg: PipelineConfig, lakehouse: Lakehouse, gold: dict, run_id: str, extra_meta: dict) -> dict:
    """Write JSON + CSV to <files_base>/exports/ via a staging folder.

    Files are first written to exports/_staging/<run_id>/ and only then moved
    into place, so a failure while writing never leaves a half-replaced set.
    """
    import os

    exports = f"{cfg.files_base}/exports"
    staging = f"{exports}/_staging/{run_id}"
    lakehouse.remove_tree(staging)
    names = {"by_year": "gold_theses_by_year", "by_university_year": "gold_theses_by_university_year",
             "completeness": "gold_metadata_completeness"}
    written = []
    for key, base in names.items():
        rows = _rows(gold[key])
        lakehouse.write_json(f"{staging}/{base}.json", rows)
        lakehouse.write_text(f"{staging}/{base}.csv", _csv(rows))
        written += [f"{base}.json", f"{base}.csv"]
    meta = {"run_id": run_id, "source_mode": cfg.source_mode, "data_scope": scope_note(cfg),
            "exported_at_utc": iso_utc(utc_now()), **extra_meta}
    lakehouse.write_json(f"{staging}/export_metadata.json", meta)
    written.append("export_metadata.json")
    for name in written:
        os.replace(lakehouse.path(f"{staging}/{name}"), lakehouse.path(f"{exports}/{name}"))
    lakehouse.remove_tree(f"{exports}/_staging")
    return {"folder": lakehouse.display(exports), "files": written}
