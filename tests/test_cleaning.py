from diva_lakehouse.cleaning import (
    MULTIPLE_UNIVERSITIES,
    UNKNOWN_UNIVERSITY,
    build_silver_candidate,
    content_hash,
    derive_university,
    normalize_abstract,
    normalize_language,
    normalize_text,
    parse_publication_year,
    pick_source_url,
)


def obs(**kw):
    base = {"record_id": "oai:DiVA.org:uu-1", "dc_title": ["  A  title\n"], "dc_description": ["Abstract."],
            "dc_subject": ["b", "a"], "dc_publisher": ["Uppsala universitet"], "dc_date": ["2021"],
            "dc_type": ["Student thesis"], "dc_identifier": ["https://www.diva-portal.org/x"],
            "dc_language": ["swe"], "set_specs": [], "run_id": "r1", "fetched_at": "2024-01-01"}
    base.update(kw)
    return base


def test_hash_is_deterministic_and_ignores_run_metadata():
    a = build_silver_candidate(obs())
    b = build_silver_candidate(obs(run_id="r2", fetched_at="2025-12-31", set_specs=["other"]))
    assert a["content_hash"] == b["content_hash"]
    assert len(a["content_hash"]) == 64


def test_hash_ignores_whitespace_and_subject_order_but_not_content():
    base = build_silver_candidate(obs())["content_hash"]
    assert build_silver_candidate(obs(dc_title=["A title"], dc_subject=["a", "b"]))["content_hash"] == base
    assert build_silver_candidate(obs(dc_description=["Abstract, revised."]))["content_hash"] != base


def test_content_hash_known_value_is_stable():
    row = {"record_id": "x", "title": "t", "titles": ["t"], "abstract": None, "abstracts": [], "subjects": ["b", "a"],
           "date_raw": ["2020"], "language_raw": ["swe"], "publishers": [], "document_types": [], "dc_identifiers": []}
    assert content_hash(row) == content_hash(dict(row, subjects=["a", "b"]))
    # Pinned value: changing the hash definition would mark every Silver row as "changed".
    assert content_hash(row) == "99f31b1f546ada78769d85c0edc02935469862f21c0e3eaf321353004841211a"


def test_text_and_abstract_normalisation():
    assert normalize_text("  a \n\t b  ") == "a b"
    assert normalize_text("   ") is None
    assert normalize_abstract("Line  one.\r\n\r\n\r\n  Line two.  ") == "Line one.\n\nLine two."


def test_multiple_subjects_and_publishers_kept():
    c = build_silver_candidate(obs(dc_subject=["x", " x ", "y"], dc_publisher=["Uppsala universitet", "Förlag AB"]))
    assert c["subjects"] == ["x", "y"]
    assert c["publishers"] == ["Uppsala universitet", "Förlag AB"]


def test_language_mapping_keeps_raw():
    assert normalize_language(["swe"]) == ("sv", ["sv"])
    assert normalize_language(["eng", "swe"]) == ("en", ["en", "sv"])
    assert normalize_language(["xx-klingon"]) == ("und", ["und"])
    c = build_silver_candidate(obs(dc_language=["Swedish"]))
    assert c["language"] == "sv" and c["language_raw"] == ["Swedish"]


def test_publication_year_is_cautious():
    assert parse_publication_year(["2021"]) == (2021, "parsed")
    assert parse_publication_year(["2021-05-01", "2021"]) == (2021, "parsed")
    assert parse_publication_year(["2020", "2021"]) == (None, "ambiguous")
    assert parse_publication_year(["ca 1990"]) == (None, "unparseable")
    assert parse_publication_year(["1700"]) == (None, "unparseable")
    assert parse_publication_year([]) == (None, "missing")


def test_university_derivation():
    assert derive_university(["Uppsala universitet, Inst. X"], None)["university"] == "Uppsala universitet"
    multi = derive_university(["Stockholms universitet", "KTH, Skolan för X"], None)
    assert multi["university"] is None and multi["university_group"] == MULTIPLE_UNIVERSITIES
    assert multi["candidates"] == ["KTH Kungliga Tekniska högskolan", "Stockholms universitet"]
    by_id = derive_university(["Some Publisher AB"], "oai:DiVA.org:liu-123")
    assert by_id["method"] == "identifier_prefix" and by_id["university"] == "Linköpings universitet"
    unknown = derive_university(["Some Publisher AB"], "oai:other:1")
    assert unknown["university"] is None and unknown["university_group"] == UNKNOWN_UNIVERSITY
    # a publisher that merely contains the letters 'kth' inside a word is not KTH
    assert derive_university(["Blekthorn förlag"], None)["university"] is None


def test_source_url_prefers_diva():
    assert pick_source_url(["urn:nbn:se:x", "https://example.org/a", "https://www.diva-portal.org/b"]) \
        == "https://www.diva-portal.org/b"
    assert pick_source_url(["urn:nbn:se:x"]) is None
