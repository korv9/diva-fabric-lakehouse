"""Pure-Python cleaning rules for Silver (testable without Spark).

All rules are documented in docs/data-dictionary.md. Original values are kept
next to every derived value.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone

# --------------------------------------------------------------------- text
_WS_INLINE = re.compile(r"[ \t\f\v  -​  　]+")
_WS_ANY = re.compile(r"\s+")


def normalize_text(value: str | None) -> str | None:
    """NFC + collapse all whitespace (incl. newlines) to one space + trim."""
    if value is None:
        return None
    value = _WS_ANY.sub(" ", unicodedata.normalize("NFC", value)).strip()
    return value or None


def normalize_abstract(value: str | None) -> str | None:
    """Whitespace-only normalisation that keeps paragraph breaks.

    Words, punctuation and order are never changed: line endings become \\n,
    runs of spaces/tabs become one space, lines are trimmed and 3+ consecutive
    line breaks become one blank line.
    """
    if value is None:
        return None
    value = unicodedata.normalize("NFC", value).replace("\r\n", "\n").replace("\r", "\n")
    lines = [_WS_INLINE.sub(" ", line).strip() for line in value.split("\n")]
    value = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()
    return value or None


def clean_list(values) -> list:
    """Normalise each value, drop empties, de-duplicate keeping first occurrence."""
    out, seen = [], set()
    for v in values or []:
        n = normalize_text(v)
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


# ----------------------------------------------------------------- language
LANGUAGE_MAP = {
    "sv": "sv", "swe": "sv", "sve": "sv", "sv-se": "sv", "svenska": "sv", "swedish": "sv",
    "en": "en", "eng": "en", "en-gb": "en", "en-us": "en", "engelska": "en", "english": "en",
    "no": "no", "nor": "no", "nb": "no", "nob": "no", "nn": "no", "nno": "no", "norska": "no",
    "norwegian": "no",
    "da": "da", "dan": "da", "danska": "da", "danish": "da",
    "fi": "fi", "fin": "fi", "finska": "fi", "finnish": "fi",
    "de": "de", "ger": "de", "deu": "de", "tyska": "de", "german": "de",
    "fr": "fr", "fre": "fr", "fra": "fr", "franska": "fr", "french": "fr",
    "es": "es", "spa": "es", "spanska": "es", "spanish": "es",
    "it": "it", "ita": "it", "italienska": "it", "italian": "it",
}
UNKNOWN_LANGUAGE = "und"


def normalize_language(values) -> tuple:
    """Return (primary_code, all_codes). Unmapped values -> 'und'."""
    codes = []
    for v in values or []:
        key = (normalize_text(v) or "").casefold()
        code = LANGUAGE_MAP.get(key, UNKNOWN_LANGUAGE)
        if code not in codes:
            codes.append(code)
    mapped = [c for c in codes if c != UNKNOWN_LANGUAGE]
    primary = mapped[0] if mapped else UNKNOWN_LANGUAGE
    return primary, codes


# ---------------------------------------------------------- publication year
_YEAR_RE = re.compile(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?(?:T[\d:.]+Z?)?$")
MIN_YEAR = 1900


def parse_publication_year(values, max_year: int | None = None) -> tuple:
    """Return (year | None, status). status in parsed|missing|unparseable|ambiguous.

    Cautious rule: only values that are a plain ISO-like date beginning with a
    four-digit year are accepted, the year must be within [1900, max_year],
    and if the accepted values disagree on the year the result is ambiguous.
    """
    max_year = max_year or datetime.now(timezone.utc).year + 1
    cleaned = [normalize_text(v) for v in values or []]
    cleaned = [v for v in cleaned if v]
    if not cleaned:
        return None, "missing"
    years = set()
    for v in cleaned:
        m = _YEAR_RE.match(v)
        if m and MIN_YEAR <= int(m.group(1)) <= max_year:
            years.add(int(m.group(1)))
    if not years:
        return None, "unparseable"
    if len(years) > 1:
        return None, "ambiguous"
    return years.pop(), "parsed"


# --------------------------------------------------------------- university
# Heuristic, documented mapping. dc:publisher in DiVA is free text and may name
# a department, a university or a publisher, so it is NOT assumed to be a
# university. A record is attributed to a university only when the text
# matches one of these name patterns (word-bounded, case-insensitive).
UNIVERSITY_PATTERNS = {
    "Uppsala universitet": ("uppsala universitet", "uppsala university"),
    "KTH Kungliga Tekniska högskolan": ("kth", "kungliga tekniska högskolan", "royal institute of technology"),
    "Stockholms universitet": ("stockholms universitet", "stockholm university"),
    "Linköpings universitet": ("linköpings universitet", "linköping university"),
    "Umeå universitet": ("umeå universitet", "umeå university"),
    "Örebro universitet": ("örebro universitet", "örebro university"),
    "Luleå tekniska universitet": ("luleå tekniska universitet", "luleå university of technology"),
    "Mittuniversitetet": ("mittuniversitetet", "mid sweden university"),
    "Karlstads universitet": ("karlstads universitet", "karlstad university"),
    "Linnéuniversitetet": ("linnéuniversitetet", "linnaeus university"),
    "Mälardalens universitet": ("mälardalens universitet", "mälardalens högskola", "mälardalen university"),
    "Malmö universitet": ("malmö universitet", "malmö högskola", "malmö university"),
    "Södertörns högskola": ("södertörns högskola", "södertörn university"),
    "Högskolan i Gävle": ("högskolan i gävle", "university of gävle"),
    "Högskolan Dalarna": ("högskolan dalarna", "dalarna university"),
    "Jönköping University": ("jönköping university", "högskolan i jönköping"),
    "Blekinge Tekniska Högskola": ("blekinge tekniska högskola", "blekinge institute of technology"),
    "Högskolan i Skövde": ("högskolan i skövde", "university of skövde"),
    "Högskolan i Borås": ("högskolan i borås", "university of borås"),
    "Högskolan i Halmstad": ("högskolan i halmstad", "halmstad university"),
    "Högskolan Kristianstad": ("högskolan kristianstad", "kristianstad university"),
    "Högskolan Väst": ("högskolan väst", "university west"),
    "Gymnastik- och idrottshögskolan": ("gymnastik- och idrottshögskolan", "swedish school of sport and health sciences"),
    "Försvarshögskolan": ("försvarshögskolan", "swedish defence university"),
}

# Secondary heuristic: DiVA OAI identifiers are commonly of the form
# "oai:DiVA.org:<code>-<number>". The code is only used when no publisher
# matched. (Not verified against the live endpoint from the build env.)
IDENTIFIER_CODE_MAP = {
    "uu": "Uppsala universitet", "kth": "KTH Kungliga Tekniska högskolan", "su": "Stockholms universitet",
    "liu": "Linköpings universitet", "umu": "Umeå universitet", "oru": "Örebro universitet",
    "ltu": "Luleå tekniska universitet", "miun": "Mittuniversitetet", "kau": "Karlstads universitet",
    "lnu": "Linnéuniversitetet", "mdh": "Mälardalens universitet", "mau": "Malmö universitet",
    "sh": "Södertörns högskola", "hig": "Högskolan i Gävle", "du": "Högskolan Dalarna",
    "hj": "Jönköping University", "bth": "Blekinge Tekniska Högskola", "his": "Högskolan i Skövde",
    "hb": "Högskolan i Borås", "hh": "Högskolan i Halmstad", "hkr": "Högskolan Kristianstad",
    "hv": "Högskolan Väst", "gih": "Gymnastik- och idrottshögskolan", "fhs": "Försvarshögskolan",
}
_DIVA_ID_RE = re.compile(r"^oai:diva\.org:([a-z]+)-\d+$", re.IGNORECASE)

UNKNOWN_UNIVERSITY = "Okänt lärosäte"
MULTIPLE_UNIVERSITIES = "Flera lärosäten (ej entydigt)"

_COMPILED_UNI = {
    name: [re.compile(r"(?<!\w)" + re.escape(p) + r"(?!\w)") for p in pats]
    for name, pats in UNIVERSITY_PATTERNS.items()
}


def derive_university(publishers, record_id: str | None) -> dict:
    """Return dict(university, university_group, method, candidates).

    university        : canonical name when exactly one university is found, else None
    university_group  : value used by Gold (never double counts a record):
                        the university, MULTIPLE_UNIVERSITIES or UNKNOWN_UNIVERSITY
    method            : publisher_match | identifier_prefix | multiple_publisher_matches | unknown
    candidates        : all universities matched in dc:publisher (sorted)
    """
    found = set()
    for p in publishers or []:
        text = (normalize_text(p) or "").casefold()
        for name, regexes in _COMPILED_UNI.items():
            if any(r.search(text) for r in regexes):
                found.add(name)
    candidates = sorted(found)
    if len(candidates) == 1:
        return {"university": candidates[0], "university_group": candidates[0],
                "method": "publisher_match", "candidates": candidates}
    if len(candidates) > 1:
        return {"university": None, "university_group": MULTIPLE_UNIVERSITIES,
                "method": "multiple_publisher_matches", "candidates": candidates}
    m = _DIVA_ID_RE.match(record_id or "")
    if m and m.group(1).lower() in IDENTIFIER_CODE_MAP:
        name = IDENTIFIER_CODE_MAP[m.group(1).lower()]
        return {"university": name, "university_group": name, "method": "identifier_prefix", "candidates": []}
    return {"university": None, "university_group": UNKNOWN_UNIVERSITY, "method": "unknown", "candidates": []}


# ----------------------------------------------------------------- links/hash
def pick_source_url(identifiers) -> str | None:
    urls = [i for i in (normalize_text(x) for x in identifiers or []) if i and i.lower().startswith(("http://", "https://"))]
    if not urls:
        return None
    diva = [u for u in urls if "diva-portal.org" in u.lower()]
    return (diva or urls)[0]


HASH_FIELDS = ("record_id", "title", "titles", "abstract", "abstracts", "subjects", "date_raw", "language_raw",
               "publishers", "document_types", "dc_identifiers")
_ORDER_INSENSITIVE = {"subjects", "publishers", "document_types", "dc_identifiers"}


def content_hash(row: dict) -> str:
    """SHA-256 over the business fields only (no run_id, no timestamps).

    Lists whose order carries no meaning are sorted, so a reordering in the
    source does not count as a change.
    """
    payload = {}
    for f in HASH_FIELDS:
        v = row.get(f)
        if isinstance(v, (list, tuple)):
            v = sorted(v) if f in _ORDER_INSENSITIVE else list(v)
        payload[f] = v
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def build_silver_candidate(obs: dict) -> dict:
    """Map one Bronze observation (dict) to Silver business fields + hash."""
    record_id = obs["record_id"]
    titles = clean_list(obs.get("dc_title"))
    abstracts = []
    for a in obs.get("dc_description") or []:
        n = normalize_abstract(a)
        if n and n not in abstracts:
            abstracts.append(n)
    publishers = clean_list(obs.get("dc_publisher"))
    date_raw = [d for d in (obs.get("dc_date") or []) if d is not None]
    language_raw = [x for x in (obs.get("dc_language") or []) if x is not None]
    lang, lang_codes = normalize_language(language_raw)
    year, year_status = parse_publication_year(date_raw)
    uni = derive_university(publishers, record_id)
    row = {
        "record_id": record_id,
        "title": titles[0] if titles else None,
        "titles": titles,
        "abstract": abstracts[0] if abstracts else None,
        "abstracts": abstracts,
        "subjects": clean_list(obs.get("dc_subject")),
        "publication_year": year,
        "publication_year_status": year_status,
        "date_raw": date_raw,
        "language": lang,
        "language_codes": lang_codes,
        "language_raw": language_raw,
        "publishers": publishers,
        "university": uni["university"],
        "university_group": uni["university_group"],
        "university_method": uni["method"],
        "university_candidates": uni["candidates"],
        "document_types": clean_list(obs.get("dc_type")),
        "dc_identifiers": clean_list(obs.get("dc_identifier")),
        "source_url": pick_source_url(obs.get("dc_identifier")),
        "set_specs": list(obs.get("set_specs") or []),
    }
    row["content_hash"] = content_hash(row)
    return row
