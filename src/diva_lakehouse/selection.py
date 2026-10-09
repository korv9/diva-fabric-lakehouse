"""Student-thesis selection based on documented dc:type values.

Rule (documented in docs/data-dictionary.md):
* A record is selected when any dc:type value contains one of
  STUDENT_THESIS_PATTERNS (case-insensitive, whitespace-normalised).
* Research degree theses (doctoral / licentiate) are explicitly NOT student
  theses and are excluded even if a generic pattern would match.
* Deleted headers have no metadata; they are kept so that Silver can mark a
  previously selected record as deleted.

The DiVA dc:type vocabulary could not be verified against the live endpoint
from the build environment; the patterns are configurable in one place.
"""

from __future__ import annotations

import re

STUDENT_THESIS_PATTERNS = (
    "student thesis",
    "studentthesis",
    "studentuppsats",
    "student paper",
    "examensarbete",
    "independent thesis",
    "bachelorthesis",
    "bachelor thesis",
    "masterthesis",
    "master thesis",
    "kandidatuppsats",
    "magisteruppsats",
    "masteruppsats",
)

EXCLUDE_PATTERNS = (
    "doctoral thesis",
    "doctoralthesis",
    "doktorsavhandling",
    "licentiate thesis",
    "licentiatethesis",
    "licentiatavhandling",
)


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().casefold()


def classify_record(types: list, deleted: bool, thesis_filter: str = "student_thesis") -> tuple:
    """Return (is_selected, reason)."""
    if deleted:
        return True, "deleted header (no metadata); kept to propagate deletion"
    if thesis_filter == "none":
        return True, "thesis_filter=none (all records selected)"
    normed = [_norm(t) for t in types or []]
    for t in normed:
        for pat in EXCLUDE_PATTERNS:
            if pat in t:
                return False, f"dc:type matched exclusion {pat!r}"
    for t in normed:
        for pat in STUDENT_THESIS_PATTERNS:
            if pat in t:
                return True, f"dc:type matched {pat!r}"
    if not normed:
        return False, "no dc:type value"
    return False, "no dc:type value matched student-thesis patterns"
