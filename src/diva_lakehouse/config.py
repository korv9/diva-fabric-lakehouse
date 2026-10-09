"""Pipeline configuration, validation and naming of tables and paths."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

# Candidate endpoint. It could NOT be verified from the build environment (see
# docs/setup-fabric.md). 01_Ingestion always calls Identify first and stops with
# a clear message if the endpoint does not answer as an OAI-PMH 2.0 repository.
DEFAULT_OAI_ENDPOINT = "https://www.diva-portal.org/dice/oai"

# Default harvest window: a short, historical, explicitly stated interval.
# NOTE: these are OAI-PMH *datestamps* (when the record was created/changed in
# DiVA), not the thesis publication year.
DEFAULT_FROM_DATESTAMP = "2024-03-01"
DEFAULT_UNTIL_DATESTAMP = "2024-03-02"

DEFAULT_MAX_RECORDS = 500
HARD_MAX_RECORDS = 1000

LIVE_TABLE_PREFIX = "diva"
DEMO_TABLE_PREFIX = "diva_demo"
LIVE_FILES_BASE = "diva"
DEMO_FILES_BASE = "diva/demo"

SOURCE_MODES = ("live", "demo")
RUN_MODES = ("pipeline", "rerun_demo")
THESIS_FILTERS = ("student_thesis", "none")

_DATESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}Z)?$")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
_PREFIX_RE = re.compile(r"^[a-z][a-z0-9_]{0,39}$")


class ConfigError(ValueError):
    """Raised when notebook parameters are invalid."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_run_id(source_mode: str, now: datetime | None = None) -> str:
    now = now or utc_now()
    return f"{source_mode}-{now.strftime('%Y%m%dT%H%M%SZ')}"


@dataclass
class PipelineConfig:
    source_mode: str = "live"
    oai_endpoint: str = DEFAULT_OAI_ENDPOINT
    metadata_prefix: str = "oai_dc"
    # "auto" = continue from the last fully harvested AND processed interval.
    from_datestamp: str = DEFAULT_FROM_DATESTAMP
    until_datestamp: str = DEFAULT_UNTIL_DATESTAMP
    set_spec: str = ""
    max_records: int = DEFAULT_MAX_RECORDS
    max_pages: int = 50
    table_prefix: str = ""  # "" -> diva (live) / diva_demo (demo)
    run_id: str = ""  # "" -> generated (01/00) or latest run (02-04)
    run_mode: str = "pipeline"  # pipeline | rerun_demo
    demo_batch: str = "A"  # demo + pipeline only: A or B
    thesis_filter: str = "student_thesis"
    reuse_existing_raw: bool = True
    request_timeout_s: float = 60.0
    max_retries: int = 3
    backoff_base_s: float = 2.0
    polite_delay_s: float = 1.0
    low_selection_threshold: int = 10
    lakehouse_root: str = "auto"  # "auto" -> /lakehouse/default (Fabric mount)
    user_agent: str = "diva-research-lakehouse/0.1 (portfolio project; OAI-PMH harvester)"
    extra: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.source_mode = (self.source_mode or "").strip().lower()
        self.run_mode = (self.run_mode or "").strip().lower()
        self.thesis_filter = (self.thesis_filter or "").strip().lower()
        self.set_spec = (self.set_spec or "").strip()
        self.run_id = (self.run_id or "").strip()
        self.demo_batch = (self.demo_batch or "").strip().upper()
        self.from_datestamp = (self.from_datestamp or "").strip()
        self.until_datestamp = (self.until_datestamp or "").strip()
        if not self.table_prefix:
            self.table_prefix = DEMO_TABLE_PREFIX if self.source_mode == "demo" else LIVE_TABLE_PREFIX
        self.table_prefix = self.table_prefix.strip().lower()
        self.validate()

    # ------------------------------------------------------------------ checks
    def validate(self) -> None:
        if self.source_mode not in SOURCE_MODES:
            raise ConfigError(f"source_mode must be one of {SOURCE_MODES}, got {self.source_mode!r}")
        if self.run_mode not in RUN_MODES:
            raise ConfigError(f"run_mode must be one of {RUN_MODES}, got {self.run_mode!r}")
        if self.run_mode == "rerun_demo" and self.source_mode != "demo":
            raise ConfigError("run_mode='rerun_demo' requires source_mode='demo' (it resets the demo tables).")
        if self.thesis_filter not in THESIS_FILTERS:
            raise ConfigError(f"thesis_filter must be one of {THESIS_FILTERS}")
        if self.source_mode == "demo" and self.demo_batch not in ("A", "B"):
            raise ConfigError("demo_batch must be 'A' or 'B'")
        if not isinstance(self.max_records, int) or not 1 <= self.max_records <= HARD_MAX_RECORDS:
            raise ConfigError(f"max_records must be an int between 1 and {HARD_MAX_RECORDS}")
        if not isinstance(self.max_pages, int) or self.max_pages < 1:
            raise ConfigError("max_pages must be a positive int")
        if not _PREFIX_RE.match(self.table_prefix):
            raise ConfigError("table_prefix must match ^[a-z][a-z0-9_]{0,39}$")
        # Live and demo data must never share tables.
        is_demo_prefix = "demo" in self.table_prefix
        if self.source_mode == "demo" and not is_demo_prefix:
            raise ConfigError("In demo mode table_prefix must contain 'demo' (keeps demo and live data apart).")
        if self.source_mode == "live" and is_demo_prefix:
            raise ConfigError("In live mode table_prefix must not contain 'demo'.")
        if self.run_id and not _RUN_ID_RE.match(self.run_id):
            raise ConfigError("run_id may only contain letters, digits, '_', '-', '.' (max 80 chars)")
        if self.source_mode == "live":
            if not self.oai_endpoint.startswith(("https://", "http://")):
                raise ConfigError("oai_endpoint must be an http(s) URL")
            if self.from_datestamp != "auto" and not _DATESTAMP_RE.match(self.from_datestamp):
                raise ConfigError("from_datestamp must be YYYY-MM-DD, YYYY-MM-DDThh:mm:ssZ or 'auto'")
            if not _DATESTAMP_RE.match(self.until_datestamp):
                raise ConfigError("until_datestamp must be YYYY-MM-DD or YYYY-MM-DDThh:mm:ssZ")
            if self.from_datestamp != "auto":
                if len(self.from_datestamp) != len(self.until_datestamp):
                    raise ConfigError("from_datestamp and until_datestamp must use the same granularity")
                if self.from_datestamp > self.until_datestamp:
                    raise ConfigError("from_datestamp must be <= until_datestamp")
        for name in ("request_timeout_s", "backoff_base_s", "polite_delay_s"):
            if float(getattr(self, name)) < 0:
                raise ConfigError(f"{name} must be >= 0")
        if self.max_retries < 0 or self.max_retries > 6:
            raise ConfigError("max_retries must be between 0 and 6")

    # ----------------------------------------------------------------- naming
    @property
    def files_base(self) -> str:
        """Base folder under the Lakehouse Files section (relative)."""
        return DEMO_FILES_BASE if self.source_mode == "demo" else LIVE_FILES_BASE

    def table(self, name: str) -> str:
        return f"{self.table_prefix}_{name}"

    @property
    def tables(self) -> dict:
        return {
            "bronze": self.table("bronze_records"),
            "quarantine": self.table("bronze_quarantine"),
            "silver": self.table("silver_theses"),
            "harvest_log": self.table("harvest_log"),
            "gold_by_year": self.table("gold_theses_by_year"),
            "gold_by_university_year": self.table("gold_theses_by_university_year"),
            "gold_completeness": self.table("gold_metadata_completeness"),
        }

    def to_dict(self) -> dict:
        return asdict(self)
