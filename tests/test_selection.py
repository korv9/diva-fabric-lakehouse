import pytest

from diva_lakehouse.selection import classify_record


@pytest.mark.parametrize("types,expected", [
    (["Student thesis"], True),
    (["info:eu-repo/semantics/bachelorThesis"], True),
    (["Studentuppsats (Examensarbete)"], True),
    (["Independent thesis Advanced level (degree of Master (Two Years))"], True),
    (["Doctoral thesis, monograph"], False),
    (["Licentiate thesis, comprehensive summary"], False),
    (["Article in journal"], False),
    ([], False),
])
def test_student_thesis_rule(types, expected):
    assert classify_record(types, deleted=False)[0] is expected


def test_reason_is_reported():
    sel, reason = classify_record(["Article"], deleted=False)
    assert not sel and "no dc:type value matched" in reason


def test_deleted_headers_are_kept():
    assert classify_record([], deleted=True)[0] is True


def test_filter_none_selects_everything():
    assert classify_record(["Article"], deleted=False, thesis_filter="none")[0] is True
