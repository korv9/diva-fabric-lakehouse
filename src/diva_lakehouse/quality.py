"""Data quality checks. BLOCKING checks stop Gold/exports; WARNINGS are reported.

Silver checks run before Gold is rebuilt; Gold checks run on the freshly
computed (not yet written) Gold DataFrames. If any blocking check fails the
previous Gold tables and exports are left untouched.
"""

from __future__ import annotations

BLOCKING = "blocking"
WARNING = "warning"


class QualityGateFailed(RuntimeError):
    def __init__(self, results):
        failed = [r["check"] for r in results if r["severity"] == BLOCKING and not r["passed"]]
        super().__init__("Blocking data quality checks failed: " + ", ".join(failed)
                         + ". Gold tables and exports were NOT replaced.")
        self.results = results


def _result(check, severity, passed, details):
    return {"check": check, "severity": severity, "passed": bool(passed), "details": details}


def _schema_signature(schema) -> list:
    return [(f.name, f.dataType.simpleString()) for f in schema.fields]


def check_silver(spark, cfg, run_stats: dict | None = None) -> list:
    from pyspark.sql import functions as F

    from .bronze import bronze_schema
    from .silver import deduplicate, silver_schema, to_candidates

    t = cfg.tables
    silver = spark.table(t["silver"])
    bronze = spark.table(t["bronze"])
    results = []

    # 1. record_id unique and non-empty
    total = silver.count()
    nulls = silver.where(F.col("record_id").isNull() | (F.trim("record_id") == "")).count()
    dups = silver.groupBy("record_id").count().where("count > 1").count()
    results.append(_result("silver_record_id_unique_not_empty", BLOCKING, nulls == 0 and dups == 0,
                           {"rows": total, "null_or_empty": nulls, "duplicated_ids": dups}))

    # 2. data types: physical schema equals the declared schema
    for name, df, expected in (("silver", silver, silver_schema()), ("bronze", bronze, bronze_schema())):
        actual, exp = _schema_signature(df.schema), _schema_signature(expected)
        results.append(_result(f"{name}_schema_types", BLOCKING, actual == exp,
                               {"mismatches": [a for a in actual if a not in exp] + [e for e in exp if e not in actual]}))
    bad_year = silver.where(F.col("publication_year").isNotNull()
                            & ~F.col("publication_year").between(1900, 2100)).count()
    bad_flags = silver.where(F.col("is_deleted").isNull() | F.col("content_hash").isNull()
                             | (F.length("content_hash") != 64)).count()
    results.append(_result("silver_value_domains", BLOCKING, bad_year == 0 and bad_flags == 0,
                           {"publication_year_out_of_range": bad_year, "bad_is_deleted_or_hash": bad_flags}))

    # 3. no older version replaced a newer one: Silver must equal the
    #    highest-priority observation of every record across ALL Bronze runs.
    latest = deduplicate(to_candidates(spark, bronze)).select(
        "record_id", F.col("content_hash").alias("l_hash"), F.col("deleted").alias("l_deleted"),
        F.col("source_datestamp").alias("l_ds"))
    cmp = silver.join(latest, "record_id", "inner")
    mismatch = cmp.where(
        (F.col("l_deleted") != F.col("is_deleted"))
        | (~F.col("l_deleted") & (F.col("l_hash") != F.col("content_hash")))
        | (F.col("source_datestamp") > F.col("l_ds"))
    )
    n_mis = mismatch.count()
    sample = [r["record_id"] for r in mismatch.select("record_id").limit(10).collect()]
    missing = latest.where(~F.col("l_deleted")).join(silver, "record_id", "left_anti").count()
    results.append(_result("silver_matches_latest_observation", BLOCKING, n_mis == 0 and missing == 0,
                           {"mismatching_records": n_mis, "sample": sample,
                            "active_latest_missing_in_silver": missing}))

    # 4. Bronze has no duplicate observations (re-runs are idempotent)
    dup_obs = bronze.groupBy("observation_id").count().where("count > 1").count()
    results.append(_result("bronze_no_duplicate_observations", BLOCKING, dup_obs == 0,
                           {"duplicated_observation_ids": dup_obs}))

    # ---- warnings
    active = silver.where(~F.col("is_deleted"))
    n_active = active.count()

    def share_missing(cond):
        n = active.where(cond).count()
        return {"missing": n, "active_records": n_active, "share": round(n / n_active, 4) if n_active else None}

    for check, cond in (
        ("warn_missing_abstract", F.col("abstract").isNull()),
        ("warn_missing_publication_year", F.col("publication_year").isNull()),
        ("warn_unidentified_university", F.col("university").isNull()),
        ("warn_missing_subjects", F.size("subjects") == 0),
    ):
        d = share_missing(cond)
        results.append(_result(check, WARNING, d["missing"] == 0, d))

    q = spark.table(t["quarantine"])
    results.append(_result("warn_quarantine", WARNING, q.count() == 0,
                           {"quarantined_total": q.count(),
                            "by_reason": {r["error_reason"]: r["count"]
                                          for r in q.groupBy("error_reason").count().limit(20).collect()}}))
    not_sel = bronze.where(~F.col("is_selected")).count()
    results.append(_result("warn_filtered_out_not_student_thesis", WARNING, True,
                           {"observations_not_selected_total": not_sel,
                            "note": "Reported, not an error: records that are not student theses are kept in "
                                    "Bronze with is_selected=false and selection_reason."}))
    if run_stats:
        silver_stats = run_stats.get("silver") or {}
        acts = silver_stats.get("actions", {})
        results.append(_result("warn_stale_or_unknown_deletes", WARNING,
                               acts.get("stale_ignored", 0) == 0 and acts.get("deleted_unknown", 0) == 0,
                               {"stale_ignored": acts.get("stale_ignored", 0),
                                "deleted_unknown": acts.get("deleted_unknown", 0)}))
        manifest = run_stats.get("manifest") or {}
        if manifest:
            results.append(_result("warn_harvest_incomplete", WARNING, bool(manifest.get("complete")),
                                   {"complete": manifest.get("complete"), "stop_reason": manifest.get("stop_reason")}))
    return results


def check_gold(spark, cfg, gold: dict) -> list:
    from pyspark.sql import functions as F

    silver = spark.table(cfg.tables["silver"])
    active = silver.where(~F.col("is_deleted")).count()
    deleted = silver.where(F.col("is_deleted")).count()
    by_year = gold["by_year"].agg(F.sum("thesis_count")).first()[0] or 0
    by_uni = gold["by_university_year"].agg(F.sum("thesis_count")).first()[0] or 0
    comp_totals = {r["records_total"] for r in gold["completeness"].select("records_total").collect()}
    results = [
        _result("gold_by_year_total_equals_active_silver", BLOCKING, by_year == active,
                {"gold_total": by_year, "silver_active": active, "silver_deleted_excluded": deleted}),
        _result("gold_by_university_year_total_equals_active_silver", BLOCKING, by_uni == active,
                {"gold_total": by_uni, "silver_active": active}),
        _result("gold_completeness_total_equals_active_silver", BLOCKING, comp_totals <= {active},
                {"records_total_values": sorted(comp_totals), "silver_active": active}),
    ]
    return results


def has_blocking_failure(results) -> bool:
    return any(r["severity"] == BLOCKING and not r["passed"] for r in results)


def summarize(results) -> dict:
    return {
        "blocking_passed": not has_blocking_failure(results),
        "blocking_failed": [r["check"] for r in results if r["severity"] == BLOCKING and not r["passed"]],
        "warnings_raised": [r["check"] for r in results if r["severity"] == WARNING and not r["passed"]],
        "checks": results,
    }
