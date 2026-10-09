import pytest

from diva_lakehouse.oai_parsing import OAIXMLError, parse_datestamp, parse_identify, parse_list_records

from .helpers import fixture


def test_identify():
    info = parse_identify(fixture("identify.xml"))
    assert info["protocolVersion"] == "2.0"
    assert info["granularity"] == "YYYY-MM-DD"
    assert info["deletedRecord"] == "persistent"


def test_list_records_namespaces_and_resumption_token():
    page = parse_list_records(fixture("list_records_page1.xml"))  # non-default namespace prefixes
    assert page.resumption_token == "token-page-2"
    assert page.complete_list_size == 3
    assert page.cursor == 0
    assert len(page.records) == 2
    rec = page.records[0]
    assert rec.identifier == "oai:DiVA.org:uu-100001"
    assert rec.datestamp.isoformat() == "2024-03-01T00:00:00+00:00"
    assert rec.set_specs == ["synthetic:studentThesis"]
    assert rec.is_valid


def test_multiple_subjects_and_publishers_kept_as_lists():
    rec = parse_list_records(fixture("list_records_page1.xml")).records[0]
    assert rec.dc["subject"] == ["alpha", "beta", "gamma"]
    assert rec.dc["publisher"] == ["Uppsala universitet, Institutionen X", "Some Publisher AB"]
    # source values are kept unchanged in Bronze (whitespace included)
    assert "Test   thesis" in rec.dc["title"][0]


def test_deleted_header():
    rec = parse_list_records(fixture("list_records_page1.xml")).records[1]
    assert rec.deleted is True
    assert rec.identifier == "oai:DiVA.org:kth-200002"
    assert rec.is_valid  # deleted records have no metadata and that is fine
    assert all(v == [] for v in rec.dc.values())


def test_last_page_has_empty_token():
    page = parse_list_records(fixture("list_records_page2.xml"))
    assert page.resumption_token is None
    assert page.complete_list_size == 3


def test_no_records_match():
    page = parse_list_records(fixture("no_records_match.xml"))
    assert page.no_records_match
    assert page.fatal_errors == []
    assert page.records == []


def test_oai_error_is_fatal():
    page = parse_list_records(fixture("bad_resumption_token.xml"))
    assert [e.code for e in page.fatal_errors] == ["badResumptionToken"]


def test_broken_xml_raises():
    with pytest.raises(OAIXMLError):
        parse_list_records(fixture("broken.xml"))


def test_non_oai_xml_raises():
    with pytest.raises(OAIXMLError):
        parse_list_records(b"<html><body>Service unavailable</body></html>")


def test_record_problems_detected():
    recs = parse_list_records(fixture("record_problems.xml")).records
    problems = [r.problems for r in recs]
    assert problems[0] == ["missing header datestamp"]
    assert problems[1][0].startswith("invalid header datestamp")
    assert problems[2] == ["missing oai_dc metadata on non-deleted record"]
    assert problems[3] == ["missing header identifier"]


@pytest.mark.parametrize("value,ok", [("2024-03-01", True), ("2024-03-01T10:00:00Z", True),
                                      ("2024/03/01", False), ("", False), (None, False)])
def test_parse_datestamp(value, ok):
    assert (parse_datestamp(value) is not None) == ok
