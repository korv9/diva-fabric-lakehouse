"""Pure-Python parsing of OAI-PMH 2.0 responses (Identify, ListRecords + oai_dc).

Only the standard library is used (xml.etree.ElementTree). Namespaces are
handled explicitly; no element is matched by prefix.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone

NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc": "http://purl.org/dc/elements/1.1/",
}

# Dublin Core elements carried into Bronze. dc:creator / dc:contributor are
# deliberately NOT kept (personal names are not needed for this project).
DC_FIELDS = ("title", "subject", "description", "publisher", "date", "type", "identifier", "language")

MAX_SNIPPET_CHARS = 4000


class OAIXMLError(ValueError):
    """The response body is not well-formed XML or not an OAI-PMH document."""


@dataclass
class OAIErrorInfo:
    code: str
    message: str


@dataclass
class ParsedRecord:
    index: int  # 0-based position within the page
    identifier: str | None
    datestamp_raw: str | None
    datestamp: datetime | None
    set_specs: list
    deleted: bool
    dc: dict  # field -> list[str]
    problems: list = field(default_factory=list)
    xml_snippet: str = ""

    @property
    def is_valid(self) -> bool:
        return not self.problems


@dataclass
class ListRecordsPage:
    records: list
    resumption_token: str | None
    complete_list_size: int | None
    cursor: int | None
    errors: list  # list[OAIErrorInfo]
    response_date: str | None

    @property
    def no_records_match(self) -> bool:
        return any(e.code == "noRecordsMatch" for e in self.errors)

    @property
    def fatal_errors(self) -> list:
        return [e for e in self.errors if e.code != "noRecordsMatch"]


def _q(prefix: str, local: str) -> str:
    return f"{{{NS[prefix]}}}{local}"


def _text(el) -> str | None:
    if el is None or el.text is None:
        return None
    value = el.text.strip()
    return value or None


def parse_datestamp(value: str | None) -> datetime | None:
    """Parse an OAI datestamp (day or second granularity, UTC)."""
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _root(xml_bytes: bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise OAIXMLError(f"Response is not well-formed XML: {exc}") from exc
    if root.tag != _q("oai", "OAI-PMH"):
        raise OAIXMLError(f"Unexpected root element {root.tag!r}; expected OAI-PMH 2.0")
    return root


def parse_errors(root) -> list:
    return [
        OAIErrorInfo(code=e.get("code", "unknown"), message=(e.text or "").strip())
        for e in root.findall("oai:error", NS)
    ]


def parse_identify(xml_bytes: bytes) -> dict:
    root = _root(xml_bytes)
    errors = parse_errors(root)
    if errors:
        raise OAIXMLError("Identify returned OAI errors: " + "; ".join(f"{e.code}: {e.message}" for e in errors))
    ident = root.find("oai:Identify", NS)
    if ident is None:
        raise OAIXMLError("Identify element missing in response")
    fields = ("repositoryName", "baseURL", "protocolVersion", "earliestDatestamp", "deletedRecord", "granularity")
    return {f: _text(ident.find(f"oai:{f}", NS)) for f in fields}


def _parse_record(index: int, rec_el) -> ParsedRecord:
    problems = []
    header = rec_el.find("oai:header", NS)
    if header is None:
        return ParsedRecord(index, None, None, None, [], False, {}, ["missing header"], _snippet(rec_el))
    deleted = header.get("status") == "deleted"
    identifier = _text(header.find("oai:identifier", NS))
    datestamp_raw = _text(header.find("oai:datestamp", NS))
    datestamp = parse_datestamp(datestamp_raw)
    set_specs = [s for s in (_text(x) for x in header.findall("oai:setSpec", NS)) if s]
    if not identifier:
        problems.append("missing header identifier")
    if not datestamp_raw:
        problems.append("missing header datestamp")
    elif datestamp is None:
        problems.append(f"invalid header datestamp {datestamp_raw!r}")

    dc_values = {f: [] for f in DC_FIELDS}
    if not deleted:
        dc_el = rec_el.find("oai:metadata/oai_dc:dc", NS)
        if dc_el is None:
            problems.append("missing oai_dc metadata on non-deleted record")
        else:
            for f in DC_FIELDS:
                for el in dc_el.findall(f"dc:{f}", NS):
                    # itertext keeps text of any (unexpected) child markup.
                    raw = "".join(el.itertext())
                    if raw.strip():
                        dc_values[f].append(raw)
    return ParsedRecord(index, identifier, datestamp_raw, datestamp, set_specs, deleted, dc_values, problems,
                        _snippet(rec_el))


def _snippet(el) -> str:
    try:
        text = ET.tostring(el, encoding="unicode")
    except Exception:  # pragma: no cover - defensive
        return ""
    return text[:MAX_SNIPPET_CHARS]


def parse_list_records(xml_bytes: bytes) -> ListRecordsPage:
    root = _root(xml_bytes)
    errors = parse_errors(root)
    response_date = _text(root.find("oai:responseDate", NS))
    lr = root.find("oai:ListRecords", NS)
    if lr is None:
        if not errors:
            raise OAIXMLError("Neither ListRecords nor error element present")
        return ListRecordsPage([], None, None, None, errors, response_date)

    records = [_parse_record(i, r) for i, r in enumerate(lr.findall("oai:record", NS))]
    token_el = lr.find("oai:resumptionToken", NS)
    token, size, cursor = None, None, None
    if token_el is not None:
        token = _text(token_el)  # empty token == last page
        size = _int_or_none(token_el.get("completeListSize"))
        cursor = _int_or_none(token_el.get("cursor"))
    return ListRecordsPage(records, token, size, cursor, errors, response_date)


def _int_or_none(value):
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None
