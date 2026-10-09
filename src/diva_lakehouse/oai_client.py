"""Small, polite OAI-PMH 2.0 HTTP client (stdlib only).

* Identify for an endpoint check.
* ListRecords with resumptionToken paging (pages are yielded as raw bytes so
  they can be stored unchanged).
* Timeouts, bounded retries with exponential backoff, Retry-After support,
  a fixed delay between requests.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

from .oai_parsing import ListRecordsPage, OAIXMLError, parse_identify, parse_list_records

RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_RETRY_AFTER_S = 120.0


class OAIHTTPError(RuntimeError):
    def __init__(self, status: int, url: str, message: str = ""):
        super().__init__(f"HTTP {status} for {url} {message}".strip())
        self.status = status
        self.url = url


class OAIProtocolError(RuntimeError):
    """The repository answered with an OAI-PMH error other than noRecordsMatch."""

    def __init__(self, errors, url: str):
        text = "; ".join(f"{e.code}: {e.message}" for e in errors)
        super().__init__(f"OAI-PMH error from {url}: {text}")
        self.errors = errors
        self.url = url


@dataclass
class HTTPResponse:
    status: int
    headers: dict
    body: bytes


def urllib_transport(url: str, timeout: float, headers: dict) -> HTTPResponse:
    """Default transport. HTTP error statuses are returned, network errors raised."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - URL is validated config
            return HTTPResponse(resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read())
    except urllib.error.HTTPError as exc:
        body = exc.read() if hasattr(exc, "read") else b""
        hdrs = {k.lower(): v for k, v in (exc.headers.items() if exc.headers else [])}
        return HTTPResponse(exc.code, hdrs, body)


@dataclass
class FetchedPage:
    page_number: int  # 1-based
    url: str
    request_params: dict
    http_status: int
    body: bytes
    parsed: ListRecordsPage | None
    parse_error: str | None
    attempts: int


class OAIClient:
    def __init__(
        self,
        endpoint: str,
        *,
        transport=urllib_transport,
        timeout_s: float = 60.0,
        max_retries: int = 3,
        backoff_base_s: float = 2.0,
        polite_delay_s: float = 1.0,
        user_agent: str = "diva-research-lakehouse",
        sleep=time.sleep,
        log=print,
    ):
        self.endpoint = endpoint
        self.transport = transport
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s
        self.polite_delay_s = polite_delay_s
        self.headers = {"User-Agent": user_agent, "Accept": "text/xml, application/xml"}
        self.sleep = sleep
        self.log = log
        self._requests_made = 0

    # ------------------------------------------------------------------ http
    def build_url(self, params: dict) -> str:
        return self.endpoint + "?" + urllib.parse.urlencode(params)

    def _get(self, params: dict) -> tuple:
        url = self.build_url(params)
        if self._requests_made and self.polite_delay_s:
            self.sleep(self.polite_delay_s)
        attempt = 0
        while True:
            attempt += 1
            self._requests_made += 1
            try:
                resp = self.transport(url, self.timeout_s, self.headers)
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
                if attempt > self.max_retries:
                    raise OAIHTTPError(0, url, f"network error after {attempt} attempts: {exc}") from exc
                wait = self.backoff_base_s * (2 ** (attempt - 1))
                self.log(f"  network error ({exc}); retry {attempt}/{self.max_retries} in {wait:.1f}s")
                self.sleep(wait)
                continue
            if resp.status == 200:
                return url, resp, attempt
            if resp.status in RETRY_STATUSES and attempt <= self.max_retries:
                wait = self._retry_after(resp) or self.backoff_base_s * (2 ** (attempt - 1))
                self.log(f"  HTTP {resp.status}; retry {attempt}/{self.max_retries} in {wait:.1f}s")
                self.sleep(wait)
                continue
            raise OAIHTTPError(resp.status, url, f"after {attempt} attempt(s)")

    @staticmethod
    def _retry_after(resp: HTTPResponse):
        value = resp.headers.get("retry-after")
        if value is None:
            return None
        try:
            return min(float(value), MAX_RETRY_AFTER_S)
        except ValueError:
            return None

    # ------------------------------------------------------------------ verbs
    def identify(self) -> dict:
        _, resp, _ = self._get({"verb": "Identify"})
        info = parse_identify(resp.body)
        if info.get("protocolVersion") != "2.0":
            raise OAIXMLError(f"Endpoint reports protocolVersion {info.get('protocolVersion')!r}, expected 2.0")
        return info

    def iter_list_records(self, metadata_prefix: str, from_: str | None, until: str | None,
                          set_spec: str | None, max_pages: int):
        """Yield FetchedPage objects until the list ends, an error occurs or max_pages is hit.

        The caller decides when to stop (e.g. at max_records) by breaking out.
        A page whose body is not parseable is yielded with parse_error set and
        iteration stops (there is no reliable resumptionToken to continue with).
        """
        params = {"verb": "ListRecords", "metadataPrefix": metadata_prefix}
        if from_:
            params["from"] = from_
        if until:
            params["until"] = until
        if set_spec:
            params["set"] = set_spec
        page_no = 0
        while page_no < max_pages:
            page_no += 1
            url, resp, attempts = self._get(params)
            try:
                parsed = parse_list_records(resp.body)
                parse_error = None
            except OAIXMLError as exc:
                parsed, parse_error = None, str(exc)
            yield FetchedPage(page_no, url, dict(params), resp.status, resp.body, parsed, parse_error, attempts)
            if parsed is None or parsed.errors or not parsed.resumption_token:
                return
            params = {"verb": "ListRecords", "resumptionToken": parsed.resumption_token}
