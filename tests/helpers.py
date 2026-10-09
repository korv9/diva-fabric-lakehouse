"""Test helpers: fixture loading, a fake HTTP transport and an OAI XML builder."""

from __future__ import annotations

import os
import urllib.parse

from diva_lakehouse.oai_client import HTTPResponse

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def fixture(name: str) -> bytes:
    with open(os.path.join(FIXTURES, name), "rb") as fh:
        return fh.read()


class FakeTransport:
    """Maps (verb, resumptionToken) -> list of responses (consumed in order; last one repeats)."""

    def __init__(self, routes: dict):
        self.routes = {k: list(v) if isinstance(v, list) else [v] for k, v in routes.items()}
        self.calls = []

    def __call__(self, url, timeout, headers):
        self.calls.append(url)
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        key = (q.get("verb", [""])[0], q.get("resumptionToken", [None])[0])
        queue = self.routes.get(key)
        if not queue:
            return HTTPResponse(404, {}, b"not found")
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        if isinstance(item, HTTPResponse):
            return item
        return HTTPResponse(200, {}, item)


def oai_page(records: list, token: str | None = None, size: int | None = None) -> bytes:
    """records: list of dicts(id, ds, deleted=False, title=..., abstract=..., types=[...], year=...)."""
    parts = []
    for r in records:
        if r.get("deleted"):
            parts.append(f'<record><header status="deleted"><identifier>{r["id"]}</identifier>'
                         f'<datestamp>{r["ds"]}</datestamp></header></record>')
            continue
        dc = [f"<dc:title>{r.get('title', 'T')}</dc:title>"]
        if r.get("abstract"):
            dc.append(f"<dc:description>{r['abstract']}</dc:description>")
        for s in r.get("subjects", []):
            dc.append(f"<dc:subject>{s}</dc:subject>")
        for p in r.get("publishers", []):
            dc.append(f"<dc:publisher>{p}</dc:publisher>")
        if r.get("year"):
            dc.append(f"<dc:date>{r['year']}</dc:date>")
        for t in r.get("types", ["Student thesis"]):
            dc.append(f"<dc:type>{t}</dc:type>")
        dc.append("<dc:language>swe</dc:language>")
        parts.append(
            f"<record><header><identifier>{r['id']}</identifier><datestamp>{r['ds']}</datestamp></header>"
            '<metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" '
            f'xmlns:dc="http://purl.org/dc/elements/1.1/">{"".join(dc)}</oai_dc:dc></metadata></record>')
    tok = ""
    if token is not None or size is not None:
        attrs = f' completeListSize="{size}"' if size is not None else ""
        tok = f"<resumptionToken{attrs}>{token or ''}</resumptionToken>"
    return ('<?xml version="1.0" encoding="UTF-8"?>\n<!-- Test data (synthetic) -->\n'
            '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><responseDate>2024-03-02T00:00:00Z</responseDate>'
            f'<request verb="ListRecords">https://oai.synthetic.example/oai</request><ListRecords>{"".join(parts)}{tok}'
            '</ListRecords></OAI-PMH>').encode()
