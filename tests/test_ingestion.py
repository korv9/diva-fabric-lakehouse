import hashlib
import os

import pytest

from diva_lakehouse.config import PipelineConfig
from diva_lakehouse.ingestion import harvest, manifest_path
from diva_lakehouse.oai_client import OAIClient, OAIProtocolError
from diva_lakehouse.storage import Lakehouse

from .helpers import FakeTransport, fixture, oai_page


@pytest.fixture
def lakehouse(tmp_path):
    os.makedirs(tmp_path / "Files")
    return Lakehouse(str(tmp_path))


def run(lakehouse, routes, **cfg_kw):
    cfg = PipelineConfig(source_mode="live", oai_endpoint="https://oai.synthetic.example/oai",
                         lakehouse_root=lakehouse.root, **cfg_kw)
    routes = {("Identify", None): fixture("identify.xml"), **routes}
    client = OAIClient(cfg.oai_endpoint, transport=FakeTransport(routes), sleep=lambda s: None, log=lambda *_: None)
    return cfg, harvest(cfg, lakehouse, client, "run-1", "2024-03-01", "2024-03-02", log=lambda *_: None)


def test_complete_harvest_stores_raw_pages_unchanged(lakehouse):
    cfg, m = run(lakehouse, {("ListRecords", None): fixture("list_records_page1.xml"),
                             ("ListRecords", "token-page-2"): fixture("list_records_page2.xml")})
    assert m["complete"] is True and m["stop_reason"] == "list_exhausted"
    assert [p["file"] for p in m["pages"]] == ["page_00001.xml", "page_00002.xml"]
    for p, name in zip(m["pages"], ["list_records_page1.xml", "list_records_page2.xml"]):
        stored = lakehouse.read_bytes(f"diva/raw/run-1/{p['file']}")
        assert stored == fixture(name)  # byte-for-byte
        assert p["sha256"] == hashlib.sha256(stored).hexdigest()
    c = m["counts"]
    assert c == {"pages": 2, "records_in_raw_pages": 3, "records_fetched": 3, "records_beyond_max_records": 0,
                 "selected_theses": 1, "not_selected": 1, "deleted_headers": 1, "invalid_records": 0}
    assert m["requested"]["from"] == "2024-03-01" and m["endpoint"] == cfg.oai_endpoint
    assert lakehouse.exists(manifest_path(cfg, "run-1"))
    assert m["fetch_started_at_utc"].endswith("Z")


def test_stop_at_max_records_is_incomplete(lakehouse):
    page = oai_page([{"id": f"oai:x:{i}", "ds": "2024-03-01"} for i in range(5)], token="next", size=50)
    _, m = run(lakehouse, {("ListRecords", None): page}, max_records=3)
    assert m["complete"] is False and m["stop_reason"] == "max_records"
    assert m["pages"][0]["records_used"] == 3 and m["counts"]["records_beyond_max_records"] == 2
    assert any("INCOMPLETE" in n for n in m["notes"])


def test_exactly_max_records_on_last_page_is_complete(lakehouse):
    page = oai_page([{"id": f"oai:x:{i}", "ds": "2024-03-01"} for i in range(3)])
    _, m = run(lakehouse, {("ListRecords", None): page}, max_records=3)
    assert m["complete"] is True


def test_max_pages_is_incomplete(lakehouse):
    p1 = oai_page([{"id": "oai:x:1", "ds": "2024-03-01"}], token="t2")
    p2 = oai_page([{"id": "oai:x:2", "ds": "2024-03-01"}], token="t3")
    _, m = run(lakehouse, {("ListRecords", None): p1, ("ListRecords", "t2"): p2}, max_pages=2)
    assert m["complete"] is False and m["stop_reason"] == "max_pages"


def test_no_records_match_is_complete_and_explained(lakehouse):
    _, m = run(lakehouse, {("ListRecords", None): fixture("no_records_match.xml")})
    assert m["complete"] is True and m["stop_reason"] == "noRecordsMatch"
    assert m["counts"]["records_fetched"] == 0
    assert any("Few student theses" in n for n in m["notes"])


def test_broken_page_is_saved_and_incomplete(lakehouse):
    _, m = run(lakehouse, {("ListRecords", None): fixture("broken.xml")})
    assert m["complete"] is False and m["stop_reason"] == "page_parse_error"
    assert lakehouse.read_bytes("diva/raw/run-1/page_00001.xml") == fixture("broken.xml")


def test_oai_error_raises_after_writing_manifest(lakehouse):
    with pytest.raises(OAIProtocolError):
        run(lakehouse, {("ListRecords", None): fixture("bad_resumption_token.xml")})
    m = lakehouse.read_json("diva/raw/run-1/manifest.json")
    assert m["complete"] is False and m["stop_reason"] == "oai_error"
