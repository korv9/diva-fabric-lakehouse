"""02_Bronze: stored raw XML pages -> Bronze observations + quarantine (Delta).

* Reads the manifest and raw pages of one run, verifies each file's SHA-256.
* Keeps source values (arrays of dc values) and full traceability.
* One Bronze row = one observation of a record in one run. The observation_id
  is derived from (run_id, raw file hash, position in page), so re-processing
  the same run_id never creates duplicate observations (insert-only MERGE).
* Records without identifier / datestamp / metadata and unparseable pages go to
  the quarantine table with a reason. Nothing is dropped silently.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from .config import PipelineConfig, iso_utc, utc_now
from .ingestion import manifest_path, raw_dir
from .oai_parsing import OAIXMLError, parse_list_records
from .selection import classify_record
from .storage import Lakehouse, sha256_bytes


class RawIntegrityError(RuntimeError):
    pass


def bronze_schema():
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    arr = ArrayType(StringType(), containsNull=False)
    return StructType([
        StructField("observation_id", StringType(), False),
        StructField("record_id", StringType(), False),
        StructField("source_datestamp", TimestampType(), False),
        StructField("source_datestamp_raw", StringType(), False),
        StructField("set_specs", arr, False),
        StructField("deleted", BooleanType(), False),
        StructField("is_selected", BooleanType(), False),
        StructField("selection_reason", StringType(), False),
        StructField("dc_title", arr, False),
        StructField("dc_subject", arr, False),
        StructField("dc_description", arr, False),
        StructField("dc_publisher", arr, False),
        StructField("dc_date", arr, False),
        StructField("dc_type", arr, False),
        StructField("dc_identifier", arr, False),
        StructField("dc_language", arr, False),
        StructField("raw_file_path", StringType(), False),
        StructField("raw_file_sha256", StringType(), False),
        StructField("record_index", IntegerType(), False),
        StructField("fetched_at", TimestampType(), False),
        StructField("run_id", StringType(), False),
        StructField("source_mode", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("harvest_complete", BooleanType(), False),
        StructField("ingested_at", TimestampType(), False),
    ])


def quarantine_schema():
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

    return StructType([
        StructField("quarantine_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("raw_file_path", StringType(), False),
        StructField("raw_file_sha256", StringType(), False),
        StructField("record_index", IntegerType(), False),  # -1 = whole page
        StructField("oai_identifier", StringType(), True),
        StructField("source_datestamp_raw", StringType(), True),
        StructField("error_reason", StringType(), False),
        StructField("raw_snippet", StringType(), True),
        StructField("fetched_at", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
    ])


def _obs_id(run_id: str, sha: str, index: int) -> str:
    return hashlib.sha256(f"{run_id}|{sha}|{index}".encode()).hexdigest()


def _parse_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def build_bronze_rows(cfg: PipelineConfig, lakehouse: Lakehouse, run_id: str) -> tuple:
    """Pure Python: return (bronze_rows, quarantine_rows, stats). No Spark needed."""
    manifest = lakehouse.read_json(manifest_path(cfg, run_id))
    ingested_at = utc_now()
    rows, quarantine = [], []
    stats = {"pages": 0, "observations": 0, "selected": 0, "not_selected": 0, "deleted": 0, "quarantined": 0,
             "records_ignored_beyond_max_records": 0}
    for page in manifest["pages"]:
        rel = f"{raw_dir(cfg, run_id)}/{page['file']}"
        body = lakehouse.read_bytes(rel)
        sha = sha256_bytes(body)
        if sha != page["sha256"]:
            raise RawIntegrityError(f"SHA-256 mismatch for {rel}: manifest {page['sha256']} != file {sha}")
        stats["pages"] += 1
        fetched_at = _parse_utc(page.get("fetched_at_utc") or manifest["fetch_started_at_utc"])
        display_path = lakehouse.display(rel)
        try:
            parsed = parse_list_records(body)
        except OAIXMLError as exc:
            quarantine.append(_q(run_id, display_path, sha, -1, None, None, f"page not parseable: {exc}",
                                 body[:4000].decode("utf-8", "replace"), fetched_at, ingested_at))
            continue
        if parsed.fatal_errors:
            reason = "; ".join(f"{e.code}: {e.message}" for e in parsed.fatal_errors)
            quarantine.append(_q(run_id, display_path, sha, -1, None, None, f"OAI error page: {reason}",
                                 body[:4000].decode("utf-8", "replace"), fetched_at, ingested_at))
            continue
        use = page.get("records_used", len(parsed.records))
        stats["records_ignored_beyond_max_records"] += max(0, len(parsed.records) - use)
        for rec in parsed.records[:use]:
            if not rec.is_valid:
                quarantine.append(_q(run_id, display_path, sha, rec.index, rec.identifier, rec.datestamp_raw,
                                     "; ".join(rec.problems), rec.xml_snippet, fetched_at, ingested_at))
                continue
            is_sel, reason = classify_record(rec.dc.get("type", []), rec.deleted, cfg.thesis_filter)
            rows.append({
                "observation_id": _obs_id(run_id, sha, rec.index),
                "record_id": rec.identifier,
                "source_datestamp": rec.datestamp,
                "source_datestamp_raw": rec.datestamp_raw,
                "set_specs": list(rec.set_specs),
                "deleted": rec.deleted,
                "is_selected": is_sel,
                "selection_reason": reason,
                **{f"dc_{f}": list(rec.dc.get(f, [])) for f in
                   ("title", "subject", "description", "publisher", "date", "type", "identifier", "language")},
                "raw_file_path": display_path,
                "raw_file_sha256": sha,
                "record_index": rec.index,
                "fetched_at": fetched_at,
                "run_id": run_id,
                "source_mode": cfg.source_mode,
                "endpoint": manifest["endpoint"],
                "harvest_complete": bool(manifest["complete"]),
                "ingested_at": ingested_at,
            })
            stats["observations"] += 1
            if rec.deleted:
                stats["deleted"] += 1
            elif is_sel:
                stats["selected"] += 1
            else:
                stats["not_selected"] += 1
    stats["quarantined"] = len(quarantine)
    stats["harvest_complete"] = bool(manifest["complete"])
    stats["manifest_counts"] = manifest["counts"]
    return rows, quarantine, stats


def _q(run_id, path, sha, index, ident, ds_raw, reason, snippet, fetched_at, ingested_at):
    return {
        "quarantine_id": _obs_id(run_id, sha, index),
        "run_id": run_id,
        "raw_file_path": path,
        "raw_file_sha256": sha,
        "record_index": index,
        "oai_identifier": ident,
        "source_datestamp_raw": ds_raw,
        "error_reason": reason,
        "raw_snippet": snippet,
        "fetched_at": fetched_at,
        "ingested_at": ingested_at,
    }


def write_bronze(spark, cfg: PipelineConfig, rows: list, quarantine: list) -> dict:
    from .spark_utils import ensure_table, merge_insert_only

    t = cfg.tables
    ensure_table(spark, t["bronze"], bronze_schema())
    ensure_table(spark, t["quarantine"], quarantine_schema())
    out = {}
    if rows:
        out["bronze_rows_inserted"] = merge_insert_only(
            spark, t["bronze"], spark.createDataFrame(rows, bronze_schema()), "observation_id")
    else:
        out["bronze_rows_inserted"] = 0
    if quarantine:
        out["quarantine_rows_inserted"] = merge_insert_only(
            spark, t["quarantine"], spark.createDataFrame(quarantine, quarantine_schema()), "quarantine_id")
    else:
        out["quarantine_rows_inserted"] = 0
    out["written_at_utc"] = iso_utc(utc_now())
    return out
