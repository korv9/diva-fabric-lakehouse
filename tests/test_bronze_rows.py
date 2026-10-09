import os

import pytest

from diva_lakehouse.bronze import RawIntegrityError, build_bronze_rows
from diva_lakehouse.config import PipelineConfig
from diva_lakehouse.ingestion import harvest
from diva_lakehouse.oai_client import OAIClient
from diva_lakehouse.storage import Lakehouse

from .helpers import FakeTransport, fixture


@pytest.fixture
def harvested(tmp_path):
    os.makedirs(tmp_path / "Files")
    lh = Lakehouse(str(tmp_path))
    cfg = PipelineConfig(source_mode="live", oai_endpoint="https://oai.synthetic.example/oai", lakehouse_root=lh.root)
    t = FakeTransport({("Identify", None): fixture("identify.xml"),
                       ("ListRecords", None): fixture("list_records_page1.xml"),
                       ("ListRecords", "token-page-2"): fixture("record_problems.xml")})
    client = OAIClient(cfg.oai_endpoint, transport=t, sleep=lambda s: None, log=lambda *_: None)
    harvest(cfg, lh, client, "run-1", "2024-03-01", "2024-03-02", log=lambda *_: None)
    return cfg, lh


def test_rows_quarantine_and_traceability(harvested):
    cfg, lh = harvested
    rows, quarantine, stats = build_bronze_rows(cfg, lh, "run-1")
    assert [r["record_id"] for r in rows] == ["oai:DiVA.org:uu-100001", "oai:DiVA.org:kth-200002"]
    assert rows[0]["raw_file_path"] == "Files/diva/raw/run-1/page_00001.xml"
    assert rows[0]["dc_subject"] == ["alpha", "beta", "gamma"]
    assert rows[1]["deleted"] is True and rows[1]["is_selected"] is True
    assert len(quarantine) == 4 and stats["quarantined"] == 4
    reasons = sorted(q["error_reason"] for q in quarantine)
    assert any("missing header identifier" in r for r in reasons)
    assert all(q["raw_file_sha256"] for q in quarantine)


def test_same_run_gives_same_observation_ids(harvested):
    cfg, lh = harvested
    a = [r["observation_id"] for r in build_bronze_rows(cfg, lh, "run-1")[0]]
    b = [r["observation_id"] for r in build_bronze_rows(cfg, lh, "run-1")[0]]
    assert a == b and len(set(a)) == len(a)


def test_tampered_raw_file_is_detected(harvested):
    cfg, lh = harvested
    lh.write_bytes("diva/raw/run-1/page_00001.xml", b"<changed/>")
    with pytest.raises(RawIntegrityError):
        build_bronze_rows(cfg, lh, "run-1")
