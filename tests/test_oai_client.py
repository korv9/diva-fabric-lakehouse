import urllib.error

import pytest

from diva_lakehouse.oai_client import HTTPResponse, OAIClient, OAIHTTPError

from .helpers import FakeTransport, fixture


def make_client(transport, **kw):
    sleeps = []
    client = OAIClient("https://oai.synthetic.example/oai", transport=transport, sleep=sleeps.append,
                       log=lambda *_: None, **kw)
    return client, sleeps


def test_identify_ok():
    client, _ = make_client(FakeTransport({("Identify", None): fixture("identify.xml")}))
    assert client.identify()["repositoryName"] == "Synthetic test repository"


def test_iterates_resumption_tokens_with_polite_delay():
    t = FakeTransport({("ListRecords", None): fixture("list_records_page1.xml"),
                       ("ListRecords", "token-page-2"): fixture("list_records_page2.xml")})
    client, sleeps = make_client(t, polite_delay_s=1.5)
    pages = list(client.iter_list_records("oai_dc", "2024-03-01", "2024-03-02", "s1", max_pages=10))
    assert [p.page_number for p in pages] == [1, 2]
    assert "from=2024-03-01" in t.calls[0] and "set=s1" in t.calls[0] and "metadataPrefix=oai_dc" in t.calls[0]
    assert "resumptionToken=token-page-2" in t.calls[1] and "metadataPrefix" not in t.calls[1]
    assert sleeps == [1.5]


def test_max_pages_stops_iteration():
    t = FakeTransport({("ListRecords", None): fixture("list_records_page1.xml"),
                       ("ListRecords", "token-page-2"): fixture("list_records_page2.xml")})
    client, _ = make_client(t, polite_delay_s=0)
    assert len(list(client.iter_list_records("oai_dc", None, None, None, max_pages=1))) == 1


def test_retry_with_backoff_and_retry_after():
    t = FakeTransport({("Identify", None): [HTTPResponse(503, {"retry-after": "7"}, b""),
                                            HTTPResponse(500, {}, b""), fixture("identify.xml")]})
    client, sleeps = make_client(t, max_retries=3, backoff_base_s=2.0)
    client.identify()
    assert sleeps == [7.0, 4.0]  # Retry-After honoured, then exponential backoff (2 * 2**1)


def test_network_errors_are_retried_then_fail():
    t = FakeTransport({("Identify", None): urllib.error.URLError("boom")})
    client, sleeps = make_client(t, max_retries=2, backoff_base_s=1.0)
    with pytest.raises(OAIHTTPError):
        client.identify()
    assert sleeps == [1.0, 2.0]


def test_client_errors_are_not_retried():
    t = FakeTransport({})
    client, sleeps = make_client(t, max_retries=3)
    with pytest.raises(OAIHTTPError) as exc:
        client.identify()
    assert exc.value.status == 404 and sleeps == [] and len(t.calls) == 1


def test_unparseable_page_is_yielded_and_stops():
    t = FakeTransport({("ListRecords", None): fixture("broken.xml")})
    client, _ = make_client(t, polite_delay_s=0)
    pages = list(client.iter_list_records("oai_dc", None, None, None, max_pages=5))
    assert len(pages) == 1 and pages[0].parsed is None and pages[0].parse_error
