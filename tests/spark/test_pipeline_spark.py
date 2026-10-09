"""Spark + Delta tests: MERGE semantics, conflicts, idempotent re-runs, quality gate."""

import itertools

import pytest

from diva_lakehouse import pipeline as P
from diva_lakehouse.config import PipelineConfig
from diva_lakehouse.oai_client import OAIClient
from diva_lakehouse.quality import QualityGateFailed

from ..helpers import FakeTransport, fixture, oai_page

_counter = itertools.count()
QUIET = lambda *_: None  # noqa: E731


def live_cfg(lakehouse, run_id, **kw):
    prefix = f"t{next(_counter)}_live"
    return PipelineConfig(source_mode="live", oai_endpoint="https://oai.synthetic.example/oai", run_id=run_id,
                          table_prefix=kw.pop("table_prefix", prefix), lakehouse_root=lakehouse.root, **kw)


def client_for(*pages):
    routes = {("Identify", None): fixture("identify.xml"), ("ListRecords", None): pages[0]}
    for i, p in enumerate(pages[1:], start=2):
        routes[("ListRecords", f"p{i}")] = p
    return OAIClient("https://oai.synthetic.example/oai", transport=FakeTransport(routes),
                     sleep=lambda s: None, log=QUIET)


def silver(spark, cfg):
    return {r["record_id"]: r.asDict() for r in spark.table(cfg.tables["silver"]).collect()}


def test_rerun_demo_end_to_end(spark, lakehouse):
    cfg = PipelineConfig(source_mode="demo", run_mode="rerun_demo", table_prefix="tdemo_demo",
                         lakehouse_root=lakehouse.root)
    result = P.run_rerun_demo(spark, cfg, lakehouse, log=QUIET)
    assert result["passed"], result["checks"]
    a = result["steps"]["1_batch_A"]["silver_changes"]
    b = result["steps"]["2_batch_B"]["silver_changes"]
    assert (a["new"], a["changed"]) == (4, 0)
    assert (b["new"], b["changed"], b["unchanged"], b["deleted"]) == (1, 1, 1, 1)
    s = silver(spark, cfg)
    assert s["oai:synthetic.example:demo-0004"]["is_deleted"] is True
    assert "version 2" in s["oai:synthetic.example:demo-0002"]["abstract"]
    # first_seen kept from batch A, last_changed moved to batch B for the changed record
    assert s["oai:synthetic.example:demo-0002"]["first_seen_run_id"] == "demo-A"
    assert s["oai:synthetic.example:demo-0002"]["last_changed_run_id"] == "demo-B"
    # unchanged record untouched by batch B
    assert s["oai:synthetic.example:demo-0003"]["last_changed_run_id"] == "demo-A"
    # Gold excludes the deleted record
    total = spark.table(cfg.tables["gold_by_year"]).groupBy().sum("thesis_count").first()[0]
    assert total == 4
    q = spark.table(cfg.tables["quarantine"]).collect()
    assert len(q) == 1 and "missing header identifier" in q[0]["error_reason"]
    exports = lakehouse.read_json("diva/demo/exports/gold_theses_by_year.json")
    assert sum(r["thesis_count"] for r in exports) == 4 and "SYNTETISK" in exports[0]["data_scope"]


def test_older_observation_never_overwrites_newer(spark, lakehouse):
    newer = oai_page([{"id": "oai:x:1", "ds": "2024-03-10", "title": "New title"}])
    older = oai_page([{"id": "oai:x:1", "ds": "2024-03-01", "title": "Old title"}])
    cfg1 = live_cfg(lakehouse, "r-new")
    P.run_pipeline(spark, cfg1, lakehouse, QUIET, client_for(newer))
    cfg2 = live_cfg(lakehouse, "r-old", table_prefix=cfg1.table_prefix)
    report = P.run_pipeline(spark, cfg2, lakehouse, QUIET, client_for(older))
    assert report["silver_changes"]["stale_ignored"] == 1 and report["silver_changes"]["changed"] == 0
    assert silver(spark, cfg2)["oai:x:1"]["title"] == "New title"
    assert report["quality"]["blocking_passed"]


def test_in_batch_duplicates_newest_wins(spark, lakehouse):
    page = oai_page([
        {"id": "oai:x:1", "ds": "2024-03-02", "title": "Second"},
        {"id": "oai:x:1", "ds": "2024-03-01", "title": "First"},
        {"id": "oai:x:2", "ds": "2024-03-01", "title": "Other"},
    ])
    cfg = live_cfg(lakehouse, "r1")
    report = P.run_pipeline(spark, cfg, lakehouse, QUIET, client_for(page))
    assert report["silver_changes"]["duplicates_removed_in_batch"] == 1
    assert silver(spark, cfg)["oai:x:1"]["title"] == "Second"


def test_delete_then_restore_and_unknown_delete(spark, lakehouse):
    cfg = live_cfg(lakehouse, "r1")
    P.run_pipeline(spark, cfg, lakehouse, QUIET, client_for(oai_page([{"id": "oai:x:1", "ds": "2024-03-01"}])))
    cfg2 = live_cfg(lakehouse, "r2", table_prefix=cfg.table_prefix)
    rep = P.run_pipeline(spark, cfg2, lakehouse, QUIET, client_for(oai_page([
        {"id": "oai:x:1", "ds": "2024-03-02", "deleted": True},
        {"id": "oai:x:999", "ds": "2024-03-02", "deleted": True}])))
    assert rep["silver_changes"]["deleted"] == 1 and rep["silver_changes"]["deleted_unknown"] == 1
    assert silver(spark, cfg)["oai:x:1"]["is_deleted"] is True
    assert "oai:x:999" not in silver(spark, cfg)
    assert spark.table(cfg.tables["gold_by_year"]).count() == 0
    cfg3 = live_cfg(lakehouse, "r3", table_prefix=cfg.table_prefix)
    rep = P.run_pipeline(spark, cfg3, lakehouse, QUIET,
                         client_for(oai_page([{"id": "oai:x:1", "ds": "2024-03-03", "title": "Back"}])))
    assert rep["silver_changes"]["changed"] == 1  # restored counts as changed in the report
    row = silver(spark, cfg)["oai:x:1"]
    assert row["is_deleted"] is False and row["title"] == "Back" and row["first_seen_run_id"] == "r1"


def test_same_run_id_twice_adds_no_bronze_rows(spark, lakehouse):
    page = oai_page([{"id": "oai:x:1", "ds": "2024-03-01"}, {"id": "oai:x:2", "ds": "2024-03-01"}])
    cfg = live_cfg(lakehouse, "same")
    P.run_pipeline(spark, cfg, lakehouse, QUIET, client_for(page))
    before = spark.table(cfg.tables["bronze"]).count()
    snap = P._snapshot(spark, cfg)
    rep = P.run_pipeline(spark, cfg, lakehouse, QUIET, client_for(page))  # raw reused, no new requests
    assert spark.table(cfg.tables["bronze"]).count() == before == 2
    assert P._snapshot(spark, cfg) == snap
    assert rep["silver_changes"]["unchanged"] == 2


def test_incomplete_harvest_does_not_move_watermark(spark, lakehouse):
    full = oai_page([{"id": "oai:x:1", "ds": "2024-03-01"}])
    cfg = live_cfg(lakehouse, "w1", from_datestamp="2024-03-01", until_datestamp="2024-03-02")
    P.run_pipeline(spark, cfg, lakehouse, QUIET, client_for(full))
    truncated = oai_page([{"id": f"oai:x:{i}", "ds": "2024-03-03"} for i in range(3)], token="p2", size=10)
    cfg2 = live_cfg(lakehouse, "w2", table_prefix=cfg.table_prefix, from_datestamp="auto",
                    until_datestamp="2024-03-05", max_records=2)
    rep = P.run_pipeline(spark, cfg2, lakehouse, QUIET, client_for(truncated, truncated))
    assert rep["harvest"]["complete"] is False and rep["harvest"]["requested"]["from"] == "2024-03-02"
    cfg3 = live_cfg(lakehouse, "w3", table_prefix=cfg.table_prefix, from_datestamp="auto",
                    until_datestamp="2024-03-05")
    ctx = P.start_run(spark, cfg3, lakehouse, QUIET)
    assert P.resolve_from_datestamp(ctx) == "2024-03-02"  # still the last COMPLETE interval


def test_blocking_failure_keeps_previous_gold_and_exports(spark, lakehouse):
    cfg = live_cfg(lakehouse, "g1")
    P.run_pipeline(spark, cfg, lakehouse, QUIET, client_for(oai_page([{"id": "oai:x:1", "ds": "2024-03-01"}])))
    exports_before = lakehouse.read_text("diva/exports/gold_theses_by_year.json")
    gold_before = P._snapshot(spark, cfg)["gold_by_year"]
    # corrupt Silver: duplicate a record_id
    s = spark.table(cfg.tables["silver"])
    s.write.format("delta").mode("append").saveAsTable(cfg.tables["silver"])
    cfg2 = live_cfg(lakehouse, "g2", table_prefix=cfg.table_prefix)
    with pytest.raises(QualityGateFailed):
        P.run_pipeline(spark, cfg2, lakehouse, QUIET,
                       client_for(oai_page([{"id": "oai:x:2", "ds": "2024-03-02"}])))
    assert lakehouse.read_text("diva/exports/gold_theses_by_year.json") == exports_before
    assert P._snapshot(spark, cfg)["gold_by_year"] == gold_before
    report = lakehouse.read_json("diva/reports/g2.json")
    assert report["status"] == "failed_blocking_quality_checks"
    assert "silver_record_id_unique_not_empty" in report["quality"]["blocking_failed"]
