"""SYNTHETIC OAI-PMH fixtures for demo mode and tests.

Every record here is invented. Titles start with "[SYNTETISK]" and identifiers
use the reserved example domain "synthetic.example". Results produced from
these fixtures are demo output, not statistics about DiVA.

Batch A (two pages, linked with a resumptionToken):
  demo-0001  new student thesis (Uppsala, Swedish, 2 subjects)
  demo-0002  new student thesis (KTH, English)
  demo-0090  journal article -> filtered out by the student-thesis rule
  demo-0003  new student thesis with publishers from two universities
  demo-0004  new student thesis without publisher/abstract
  (no id)    broken record without identifier -> quarantine
Batch B (one page):
  demo-0002  changed (revised abstract, newer datestamp)
  demo-0005  new
  demo-0003  unchanged content, newer datestamp
  demo-0004  deleted header
"""

from __future__ import annotations

import urllib.parse

from .oai_client import HTTPResponse

SYNTHETIC_ENDPOINT = "synthetic://demo-oai"

_HEAD = """<?xml version="1.0" encoding="UTF-8"?>
<!-- SYNTHETIC DEMO DATA - not real DiVA records -->
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <responseDate>{response_date}</responseDate>
  <request verb="ListRecords" metadataPrefix="oai_dc">synthetic://demo-oai</request>
"""
_TAIL = "</OAI-PMH>\n"

_DC_OPEN = ('<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" '
            'xmlns:dc="http://purl.org/dc/elements/1.1/">')


def _record(identifier, datestamp, dc_xml, set_spec="synthetic:studentThesis"):
    ident = f"<identifier>{identifier}</identifier>" if identifier else ""
    return f"""  <record>
    <header>
      {ident}
      <datestamp>{datestamp}</datestamp>
      <setSpec>{set_spec}</setSpec>
    </header>
    <metadata>
      {_DC_OPEN}
{dc_xml}
      </oai_dc:dc>
    </metadata>
  </record>
"""


def _deleted(identifier, datestamp):
    return f"""  <record>
    <header status="deleted">
      <identifier>{identifier}</identifier>
      <datestamp>{datestamp}</datestamp>
    </header>
  </record>
"""


_STUDENT_TYPES = """        <dc:type>Student thesis</dc:type>
        <dc:type>info:eu-repo/semantics/bachelorThesis</dc:type>
        <dc:type>text</dc:type>"""

_R1 = _record("oai:synthetic.example:demo-0001", "2024-03-01T08:15:00Z", f"""        <dc:title>[SYNTETISK] Öppna data i kommunal planering</dc:title>
        <dc:title>[SYNTHETIC] Open data in municipal planning</dc:title>
        <dc:subject>öppna data</dc:subject>
        <dc:subject>kommunal planering</dc:subject>
        <dc:subject>öppna data</dc:subject>
        <dc:description>Syntetisk sammanfattning.   Den här uppsatsen finns inte på riktigt.</dc:description>
        <dc:publisher>Uppsala universitet, Institutionen för informatik och media</dc:publisher>
        <dc:date>2021</dc:date>
{_STUDENT_TYPES}
        <dc:identifier>https://synthetic.example/record/demo-0001</dc:identifier>
        <dc:language>swe</dc:language>""")

_R2_A = _record("oai:synthetic.example:demo-0002", "2024-03-01T09:00:00Z", f"""        <dc:title>[SYNTHETIC] Energy use in student housing</dc:title>
        <dc:subject>energy</dc:subject>
        <dc:description>Synthetic abstract, version 1.</dc:description>
        <dc:publisher>KTH, School of Architecture and the Built Environment (ABE)</dc:publisher>
        <dc:date>2022-06-10</dc:date>
{_STUDENT_TYPES}
        <dc:identifier>https://synthetic.example/record/demo-0002</dc:identifier>
        <dc:language>eng</dc:language>""")

_R90 = _record("oai:synthetic.example:demo-0090", "2024-03-01T09:30:00Z", """        <dc:title>[SYNTHETIC] A journal article that is not a student thesis</dc:title>
        <dc:publisher>Uppsala universitet</dc:publisher>
        <dc:date>2023</dc:date>
        <dc:type>Article in journal</dc:type>
        <dc:type>info:eu-repo/semantics/article</dc:type>
        <dc:language>eng</dc:language>""", set_spec="synthetic:article")

_R3 = _record("oai:synthetic.example:demo-0003", "2024-03-01T10:00:00Z", f"""        <dc:title>[SYNTETISK] Samverkan mellan två lärosäten</dc:title>
        <dc:subject>samverkan</dc:subject>
        <dc:subject>högre utbildning</dc:subject>
        <dc:description>Syntetisk sammanfattning om samverkan.</dc:description>
        <dc:publisher>Stockholms universitet</dc:publisher>
        <dc:publisher>Södertörns högskola</dc:publisher>
        <dc:date>2022-06-01</dc:date>
{_STUDENT_TYPES}
        <dc:language>sv</dc:language>""")

_R4 = _record("oai:synthetic.example:demo-0004", "2024-03-01T11:00:00Z", f"""        <dc:title>[SYNTETISK] Uppsats utan abstract och utgivare</dc:title>
        <dc:date>2023-01-15</dc:date>
{_STUDENT_TYPES}
        <dc:language>swe</dc:language>""")

_BROKEN = _record(None, "2024-03-01T11:30:00Z", f"""        <dc:title>[SYNTETISK] Trasig post utan identifierare</dc:title>
{_STUDENT_TYPES}""")

_R2_B = _record("oai:synthetic.example:demo-0002", "2024-03-05T12:00:00Z", f"""        <dc:title>[SYNTHETIC] Energy use in student housing</dc:title>
        <dc:subject>energy</dc:subject>
        <dc:description>Synthetic abstract, version 2 (revised by the author).</dc:description>
        <dc:publisher>KTH, School of Architecture and the Built Environment (ABE)</dc:publisher>
        <dc:date>2022-06-10</dc:date>
{_STUDENT_TYPES}
        <dc:identifier>https://synthetic.example/record/demo-0002</dc:identifier>
        <dc:language>eng</dc:language>""")

_R3_B = _R3.replace("2024-03-01T10:00:00Z", "2024-03-05T12:10:00Z")

_R5 = _record("oai:synthetic.example:demo-0005", "2024-03-05T12:20:00Z", f"""        <dc:title>[SYNTETISK] Digital tillgänglighet på bibliotek</dc:title>
        <dc:subject>tillgänglighet</dc:subject>
        <dc:subject>bibliotek</dc:subject>
        <dc:description>Syntetisk sammanfattning.

Andra stycket i abstract.</dc:description>
        <dc:publisher>Linköpings universitet, Institutionen för kultur och samhälle</dc:publisher>
        <dc:date>2024</dc:date>
{_STUDENT_TYPES}
        <dc:language>swe</dc:language>""")

_R4_DEL = _deleted("oai:synthetic.example:demo-0004", "2024-03-05T12:30:00Z")


def _page(response_date, records, token_xml):
    return (_HEAD.format(response_date=response_date) + "  <ListRecords>\n" + "".join(records)
            + token_xml + "  </ListRecords>\n" + _TAIL).encode("utf-8")


BATCH_A_PAGES = [
    _page("2024-03-02T00:00:00Z", [_R1, _R2_A, _R90],
          '    <resumptionToken completeListSize="6" cursor="0">demoA-page2</resumptionToken>\n'),
    _page("2024-03-02T00:00:01Z", [_R3, _R4, _BROKEN],
          '    <resumptionToken completeListSize="6" cursor="3"/>\n'),
]
BATCH_B_PAGES = [
    _page("2024-03-06T00:00:00Z", [_R2_B, _R5, _R3_B, _R4_DEL], ""),
]
BATCHES = {"A": BATCH_A_PAGES, "B": BATCH_B_PAGES}

IDENTIFY_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2024-03-01T00:00:00Z</responseDate>
  <request verb="Identify">synthetic://demo-oai</request>
  <Identify>
    <repositoryName>SYNTHETIC demo repository (not DiVA)</repositoryName>
    <baseURL>synthetic://demo-oai</baseURL>
    <protocolVersion>2.0</protocolVersion>
    <adminEmail>nobody@synthetic.example</adminEmail>
    <earliestDatestamp>2024-01-01T00:00:00Z</earliestDatestamp>
    <deletedRecord>persistent</deletedRecord>
    <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>
  </Identify>
</OAI-PMH>
"""

# Expected outcome (used by tests and the re-run demo):
EXPECTED_AFTER_A = {"silver_rows": 4, "silver_active": 4, "quarantine": 1, "not_selected": 1}
EXPECTED_AFTER_B = {"silver_rows": 5, "silver_active": 4, "new": 1, "changed": 1, "unchanged": 1, "deleted": 1}


class SyntheticTransport:
    """Serves the synthetic pages through the same client code as live mode."""

    def __init__(self, batch: str):
        self.pages = BATCHES[batch]
        self.calls = []

    def __call__(self, url, timeout, headers):
        self.calls.append(url)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        verb = query.get("verb", [""])[0]
        if verb == "Identify":
            return HTTPResponse(200, {}, IDENTIFY_XML)
        if verb == "ListRecords":
            token = query.get("resumptionToken", [None])[0]
            if token is None:
                return HTTPResponse(200, {}, self.pages[0])
            if token == "demoA-page2" and len(self.pages) > 1:
                return HTTPResponse(200, {}, self.pages[1])
        return HTTPResponse(400, {}, b"unsupported synthetic request")
