"""Stage orchestration used by the notebooks (00_Run_All and 01-04).

State between notebooks is kept in the Lakehouse Files section, so 01-04 can
run as separate notebooks/sessions in order, while 00_Run_All runs everything
in one session.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace

from . import bronze as bronze_mod
from . import gold as gold_mod
from . import quality as quality_mod
from . import silver as silver_mod
from .config import ConfigError, PipelineConfig, iso_utc, new_run_id, utc_now
from .demo_data import SYNTHETIC_ENDPOINT, SyntheticTransport
from .ingestion import harvest, manifest_path
from .oai_client import OAIClient, urllib_transport
from .storage import Lakehouse


@dataclass
class RunContext:
    spark: object
    cfg: PipelineConfig
    lakehouse: Lakehouse
    run_id: str
    state: dict = field(default_factory=dict)
    log: object = print

    def rel(self, *parts) -> str:
        return "/".join([self.cfg.files_base, *parts])

    @property
    def state_dir(self) -> str:
        return self.rel("state", self.run_id)

    def save_state(self, name: str, obj) -> None:
        self.lakehouse.write_json(f"{self.state_dir}/{name}.json", obj)

    def load_state(self, name: str):
        p = f"{self.state_dir}/{name}.json"
        return self.lakehouse.read_json(p) if self.lakehouse.exists(p) else None


# ------------------------------------------------------------------ run setup
def start_run(spark, cfg: PipelineConfig, lakehouse: Lakehouse | None = None, log=print) -> RunContext:
    """Used by 00 and 01: check the Lakehouse, fix the run_id, record start time."""
    lakehouse = lakehouse or Lakehouse(cfg.lakehouse_root)
    lakehouse.check_available()
    run_id = cfg.run_id or new_run_id(cfg.source_mode)
    ctx = RunContext(spark, cfg, lakehouse, run_id, log=log)
    state = ctx.load_state("run_state")
    if state is None:
        state = {"run_id": run_id, "source_mode": cfg.source_mode, "run_mode": cfg.run_mode,
                 "started_at_utc": iso_utc(utc_now()), "config": cfg.to_dict()}
        ctx.save_state("run_state", state)
    ctx.state = state
    lakehouse.write_text(ctx.rel("state", "latest_run_id.txt"), run_id)
    log(f"Run {run_id} | mode={cfg.source_mode}/{cfg.run_mode} | tables prefix={cfg.table_prefix}_* | "
        f"files={lakehouse.display(cfg.files_base)}")
    return ctx


def resume_run(spark, cfg: PipelineConfig, lakehouse: Lakehouse | None = None, log=print) -> RunContext:
    """Used by 02-04: continue the run given by run_id, or the latest started run."""
    lakehouse = lakehouse or Lakehouse(cfg.lakehouse_root)
    lakehouse.check_available()
    run_id = cfg.run_id
    pointer = f"{cfg.files_base}/state/latest_run_id.txt"
    if not run_id:
        if not lakehouse.exists(pointer):
            raise ConfigError("No run_id given and no previous run found. Run 01_Ingestion first.")
        run_id = lakehouse.read_text(pointer).strip()
    ctx = RunContext(spark, cfg, lakehouse, run_id, log=log)
    ctx.state = ctx.load_state("run_state") or {}
    if not ctx.state:
        raise ConfigError(f"Run {run_id} has no state in {lakehouse.display(ctx.state_dir)}. Run 01_Ingestion first.")
    log(f"Continuing run {run_id} ({cfg.source_mode})")
    return ctx


# ------------------------------------------------------------------ harvest log
def _harvest_log_schema():
    from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType, TimestampType

    return StructType([
        StructField("run_id", StringType(), False),
        StructField("source_mode", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("set_spec", StringType(), False),
        StructField("from_datestamp", StringType(), True),
        StructField("until_datestamp", StringType(), True),
        StructField("complete", BooleanType(), False),
        StructField("stop_reason", StringType(), True),
        StructField("records_fetched", IntegerType(), False),
        StructField("selected_theses", IntegerType(), False),
        StructField("fetched_at", TimestampType(), False),
        StructField("processed", BooleanType(), False),  # True once Silver MERGE succeeded
    ])


def _upsert_harvest_log(ctx: RunContext, manifest: dict, processed: bool) -> None:
    from datetime import datetime, timezone

    from delta.tables import DeltaTable

    from .spark_utils import ensure_table

    name = ctx.cfg.tables["harvest_log"]
    ensure_table(ctx.spark, name, _harvest_log_schema())
    req = manifest["requested"]
    row = {
        "run_id": ctx.run_id, "source_mode": ctx.cfg.source_mode, "endpoint": manifest["endpoint"],
        "set_spec": req.get("set") or "", "from_datestamp": req.get("from"), "until_datestamp": req.get("until"),
        "complete": bool(manifest["complete"]), "stop_reason": manifest.get("stop_reason"),
        "records_fetched": int(manifest["counts"]["records_fetched"]),
        "selected_theses": int(manifest["counts"]["selected_theses"]),
        "fetched_at": datetime.strptime(manifest["fetch_started_at_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc),
        "processed": processed,
    }
    df = ctx.spark.createDataFrame([row], _harvest_log_schema())
    (DeltaTable.forName(ctx.spark, name).alias("t").merge(df.alias("s"), "t.run_id = s.run_id")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def resolve_from_datestamp(ctx: RunContext) -> str:
    """'auto' -> until of the last COMPLETE and PROCESSED harvest (same endpoint/set).

    Incomplete harvests (stopped at max_records/max_pages, errors) never move
    the watermark. The interval boundary day is re-requested (from = previous
    until); overlaps are harmless because Bronze/Silver are idempotent.
    """
    cfg = ctx.cfg
    if cfg.from_datestamp != "auto":
        return cfg.from_datestamp
    from .spark_utils import table_exists

    name = cfg.tables["harvest_log"]
    if not table_exists(ctx.spark, name):
        raise ConfigError("from_datestamp='auto' needs at least one completed harvest. Give an explicit date.")
    from pyspark.sql import functions as F

    row = (ctx.spark.table(name)
           .where(F.col("complete") & F.col("processed") & (F.col("endpoint") == cfg.oai_endpoint)
                  & (F.col("set_spec") == cfg.set_spec))
           .agg(F.max("until_datestamp")).first())
    if row is None or row[0] is None:
        raise ConfigError("from_datestamp='auto': no completed+processed harvest found. Give an explicit date.")
    return row[0]


# ------------------------------------------------------------------ stages
def make_client(cfg: PipelineConfig, log=print) -> OAIClient:
    if cfg.source_mode == "demo":
        return OAIClient(SYNTHETIC_ENDPOINT, transport=SyntheticTransport(cfg.demo_batch), polite_delay_s=0,
                         max_retries=0, log=log)
    return OAIClient(cfg.oai_endpoint, transport=urllib_transport, timeout_s=cfg.request_timeout_s,
                     max_retries=cfg.max_retries, backoff_base_s=cfg.backoff_base_s,
                     polite_delay_s=cfg.polite_delay_s, user_agent=cfg.user_agent, log=log)


def stage_ingestion(ctx: RunContext, client: OAIClient | None = None) -> dict:
    cfg, lh = ctx.cfg, ctx.lakehouse
    mpath = manifest_path(cfg, ctx.run_id)
    if cfg.reuse_existing_raw and lh.exists(mpath):
        manifest = lh.read_json(mpath)
        ctx.log(f"Raw data for run {ctx.run_id} already exists; reusing {lh.display(mpath)} (no new requests).")
    else:
        if cfg.source_mode == "demo":
            from_ds, until_ds = None, None
        else:
            from_ds, until_ds = resolve_from_datestamp(ctx), cfg.until_datestamp
            if from_ds > until_ds:
                raise ConfigError(f"Resolved from_datestamp {from_ds} is after until_datestamp {until_ds}.")
        client = client or make_client(cfg, ctx.log)
        ctx.log(f"Harvesting from={from_ds} until={until_ds} set={cfg.set_spec or '-'} max_records={cfg.max_records}")
        manifest = harvest(cfg, lh, client, ctx.run_id, from_ds, until_ds, log=ctx.log)
    _upsert_harvest_log(ctx, manifest, processed=False)
    c = manifest["counts"]
    ctx.log(f"Ingestion: complete={manifest['complete']} ({manifest['stop_reason']}), pages={c['pages']}, "
            f"fetched={c['records_fetched']}, selected={c['selected_theses']}, deleted={c['deleted_headers']}, "
            f"not_selected={c['not_selected']}, invalid={c['invalid_records']}")
    return manifest


def stage_bronze(ctx: RunContext) -> dict:
    rows, quarantine, stats = bronze_mod.build_bronze_rows(ctx.cfg, ctx.lakehouse, ctx.run_id)
    stats.update(bronze_mod.write_bronze(ctx.spark, ctx.cfg, rows, quarantine))
    ctx.save_state("bronze", stats)
    ctx.log(f"Bronze: observations={stats['observations']} (inserted {stats['bronze_rows_inserted']}), "
            f"quarantined={stats['quarantined']} (inserted {stats['quarantine_rows_inserted']})")
    return stats


def stage_silver(ctx: RunContext) -> dict:
    stats = silver_mod.run_silver(ctx.spark, ctx.cfg, ctx.run_id)
    ctx.save_state("silver", stats)
    manifest = ctx.lakehouse.read_json(manifest_path(ctx.cfg, ctx.run_id))
    _upsert_harvest_log(ctx, manifest, processed=True)
    ctx.log("Silver: " + ", ".join(f"{k}={v}" for k, v in stats["actions"].items())
            + f" | duplicates removed in batch={stats['duplicates_removed_in_batch']}")
    return stats


def stage_quality_gold(ctx: RunContext) -> dict:
    """Blocking checks -> Gold (computed) -> Gold checks -> write Gold + exports -> report."""
    cfg, spark = ctx.cfg, ctx.spark
    manifest = ctx.lakehouse.read_json(manifest_path(cfg, ctx.run_id))
    run_stats = {"manifest": manifest, "silver": ctx.load_state("silver"), "bronze": ctx.load_state("bronze")}
    results = quality_mod.check_silver(spark, cfg, run_stats)
    gold_info = {"written": False}
    exports = None
    if not quality_mod.has_blocking_failure(results):
        gold = gold_mod.build_gold(spark, cfg, ctx.run_id)
        results += quality_mod.check_gold(spark, cfg, gold)
        if not quality_mod.has_blocking_failure(results):
            gold_mod.write_gold(cfg, gold)
            gold_info = {"written": True, "row_counts": {k: gold[k].count() for k in gold}}
            exports = gold_mod.export_gold(cfg, ctx.lakehouse, gold, ctx.run_id,
                                           {"harvested_intervals": harvested_intervals(ctx)})
    summary = quality_mod.summarize(results)
    ctx.save_state("quality", summary)
    report = write_run_report(ctx, manifest, run_stats, summary, gold_info, exports)
    print_quality(summary, ctx.log)
    if not summary["blocking_passed"]:
        raise quality_mod.QualityGateFailed(results)
    return report


def harvested_intervals(ctx: RunContext) -> list:
    rows = ctx.spark.table(ctx.cfg.tables["harvest_log"]).orderBy("fetched_at").collect()
    return [{"run_id": r["run_id"], "from": r["from_datestamp"], "until": r["until_datestamp"],
             "set": r["set_spec"], "complete": r["complete"], "processed": r["processed"],
             "records_fetched": r["records_fetched"]} for r in rows]


def write_run_report(ctx, manifest, run_stats, quality_summary, gold_info, exports) -> dict:
    silver = run_stats.get("silver") or {}
    bronze = run_stats.get("bronze") or {}
    acts = silver.get("actions", {})
    report = {
        "run_id": ctx.run_id,
        "source_mode": ctx.cfg.source_mode,
        "run_mode": ctx.cfg.run_mode,
        "data_label": gold_mod.scope_note(ctx.cfg),
        "started_at_utc": ctx.state.get("started_at_utc"),
        "finished_at_utc": iso_utc(utc_now()),
        "status": "succeeded" if quality_summary["blocking_passed"] else "failed_blocking_quality_checks",
        "harvest": {
            "endpoint": manifest["endpoint"],
            "requested": manifest["requested"],
            "complete": manifest["complete"],
            "stop_reason": manifest["stop_reason"],
            "counts": manifest["counts"],
            "notes": manifest.get("notes", []),
        },
        "records": {
            "fetched": manifest["counts"]["records_fetched"],
            "accepted_into_bronze": bronze.get("observations"),
            "selected_student_theses": bronze.get("selected"),
            "not_selected": bronze.get("not_selected"),
            "deleted_headers": bronze.get("deleted"),
            "quarantined": bronze.get("quarantined"),
        },
        "silver_changes": {
            "new": acts.get("new", 0),
            "changed": acts.get("changed", 0) + acts.get("restored", 0),
            "unchanged": acts.get("unchanged", 0),
            "deleted": acts.get("deleted", 0),
            "stale_ignored": acts.get("stale_ignored", 0),
            "deleted_unknown": acts.get("deleted_unknown", 0),
            "duplicates_removed_in_batch": silver.get("duplicates_removed_in_batch", 0),
        },
        "quality": {k: quality_summary[k] for k in ("blocking_passed", "blocking_failed", "warnings_raised")},
        "quality_checks": quality_summary["checks"],
        "gold": gold_info,
        "exports": exports,
    }
    path = ctx.rel("reports", f"{ctx.run_id}.json")
    ctx.lakehouse.write_json(path, report)
    report["report_path"] = ctx.lakehouse.display(path)
    ctx.log(f"Run report: {report['report_path']} (status={report['status']})")
    return report


def print_quality(summary: dict, log=print) -> None:
    for r in summary["checks"]:
        mark = "PASS" if r["passed"] else ("FAIL" if r["severity"] == "blocking" else "WARN")
        log(f"  [{mark}] {r['severity']:8s} {r['check']}: {json.dumps(r['details'], ensure_ascii=False, default=str)}")


def run_pipeline(spark, cfg: PipelineConfig, lakehouse: Lakehouse | None = None, log=print,
                 client: OAIClient | None = None) -> dict:
    ctx = start_run(spark, cfg, lakehouse, log)
    stage_ingestion(ctx, client)
    stage_bronze(ctx)
    stage_silver(ctx)
    return stage_quality_gold(ctx)


# ------------------------------------------------------------------ re-run demo
def _snapshot(spark, cfg: PipelineConfig) -> dict:
    def rows(name, drop=()):
        df = spark.table(name).drop(*drop)
        return sorted(json.dumps(r.asDict(recursive=True), sort_keys=True, default=str) for r in df.collect())

    t = cfg.tables
    return {
        "silver": rows(t["silver"]),
        "gold_by_year": rows(t["gold_by_year"], ("built_by_run_id",)),
        "gold_by_university_year": rows(t["gold_by_university_year"], ("built_by_run_id",)),
        "gold_completeness": rows(t["gold_completeness"], ("built_by_run_id",)),
    }


def reset_demo(spark, cfg: PipelineConfig, lakehouse: Lakehouse) -> None:
    from .spark_utils import drop_table

    if cfg.source_mode != "demo" or "demo" not in cfg.table_prefix or "demo" not in cfg.files_base:
        raise ConfigError("Refusing to reset: not a demo configuration.")
    for name in cfg.tables.values():
        drop_table(spark, name)
    lakehouse.remove_tree(cfg.files_base)


def run_rerun_demo(spark, cfg: PipelineConfig, lakehouse: Lakehouse | None = None, log=print) -> dict:
    """SYNTHETIC demo: A -> B -> B again (new run_id) -> re-process run demo-B (same run_id).

    Verifies that Silver and Gold are logically identical after steps 2, 3, 4
    and that re-processing the same run_id adds no Bronze observations.
    """
    from pyspark.sql import functions as F

    from .demo_data import EXPECTED_AFTER_B

    lakehouse = lakehouse or Lakehouse(cfg.lakehouse_root)
    lakehouse.check_available()
    reset_demo(spark, cfg, lakehouse)
    t = cfg.tables
    steps = [("1_batch_A", "A", "demo-A"), ("2_batch_B", "B", "demo-B"), ("3_batch_B_again", "B", "demo-B-repeat")]
    snaps, out = {}, {"label": "SYNTHETIC DEMO DATA", "steps": {}}
    for step, batch, run_id in steps:
        log(f"\n=== Demo step {step}: batch {batch}, run_id={run_id} ===")
        step_cfg = replace(cfg, run_id=run_id, demo_batch=batch, run_mode="rerun_demo")
        report = run_pipeline(spark, step_cfg, lakehouse, log)
        snaps[step] = _snapshot(spark, cfg)
        out["steps"][step] = {"run_id": run_id, "silver_changes": report["silver_changes"],
                              "bronze_rows_total": spark.table(t["bronze"]).count(),
                              "status": report["status"]}

    log("\n=== Demo step 4_reprocess_same_run_id: Bronze+Silver+Gold for run_id=demo-B again ===")
    step_cfg = replace(cfg, run_id="demo-B", demo_batch="B", run_mode="rerun_demo")
    ctx = resume_run(spark, step_cfg, lakehouse, log)
    stage_ingestion(ctx)  # reuses stored raw pages, no new requests
    stage_bronze(ctx)
    stage_silver(ctx)
    report = stage_quality_gold(ctx)
    snaps["4_reprocess_same_run_id"] = _snapshot(spark, cfg)
    out["steps"]["4_reprocess_same_run_id"] = {"run_id": "demo-B", "silver_changes": report["silver_changes"],
                                               "bronze_rows_total": spark.table(t["bronze"]).count(),
                                               "status": report["status"]}

    s2, s3, s4 = snaps["2_batch_B"], snaps["3_batch_B_again"], snaps["4_reprocess_same_run_id"]
    silver = spark.table(t["silver"])
    b_changes = out["steps"]["2_batch_B"]["silver_changes"]
    checks = {
        "silver_identical_after_steps_2_3_4": s2["silver"] == s3["silver"] == s4["silver"],
        "gold_identical_after_steps_2_3_4": all(s2[k] == s3[k] == s4[k] for k in s2 if k.startswith("gold")),
        "bronze_unchanged_when_same_run_id_reprocessed":
            out["steps"]["3_batch_B_again"]["bronze_rows_total"]
            == out["steps"]["4_reprocess_same_run_id"]["bronze_rows_total"],
        "step3_and_4_report_no_changes": all(
            out["steps"][s]["silver_changes"][k] == 0 for s in ("3_batch_B_again", "4_reprocess_same_run_id")
            for k in ("new", "changed", "deleted")),
        "batch_B_changes_as_designed": all(b_changes[k] == EXPECTED_AFTER_B[k]
                                           for k in ("new", "changed", "unchanged", "deleted")),
        "silver_rows_as_designed": silver.count() == EXPECTED_AFTER_B["silver_rows"]
        and silver.where(~F.col("is_deleted")).count() == EXPECTED_AFTER_B["silver_active"],
        "no_duplicate_record_ids": silver.groupBy("record_id").count().where("count > 1").count() == 0,
    }
    out["checks"] = checks
    out["passed"] = all(checks.values())
    out["finished_at_utc"] = iso_utc(utc_now())
    path = f"{cfg.files_base}/reports/rerun_demo_result.json"
    lakehouse.write_json(path, out)
    log("\n=== Re-run demo result (SYNTHETIC) ===")
    for k, v in checks.items():
        log(f"  [{'PASS' if v else 'FAIL'}] {k}")
    log(f"Saved: {lakehouse.display(path)}")
    if not out["passed"]:
        raise AssertionError("Re-run demo checks failed: " + ", ".join(k for k, v in checks.items() if not v))
    return out
