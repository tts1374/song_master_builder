"""Fixture tests for Textage JS parsing and decoding."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.textage_loader import (
    _charset_from_content_type,
    _decode_textage_response,
    _extract_js_object,
)


@pytest.mark.light
def test_extract_js_object_with_minimal_titletbl():
    """titletbl constants and object parsing work for minimal fixture."""
    js = """
    SS=35;
    titletbl={
      "k1":[SS,"T001","","GENRE","ARTIST","TITLE"]
    };
    """
    parsed = _extract_js_object(js, "titletbl")
    assert parsed["k1"][0] == "-35"
    assert parsed["k1"][1] == "T001"


@pytest.mark.light
def test_extract_js_object_with_minimal_datatbl_and_actbl():
    """datatbl/actbl minimal objects are parsed."""
    data_js = """
    datatbl={
      "k1":[0,101,102,103,104,105,106,107,108,109,110]
    };
    """
    act_js = """
    actbl={
      "k1":[3,0,5,0,5,0,5,0,5,0,5,0,0,0,5,0,5,0,5,0,5,0]
    };
    """
    datatbl = _extract_js_object(data_js, "datatbl")
    actbl = _extract_js_object(act_js, "actbl")
    assert datatbl["k1"][1] == 101
    assert actbl["k1"][0] == 3


@pytest.mark.light
def test_extract_js_object_raises_for_missing_varname():
    """Missing variable name raises RuntimeError."""
    js = "var a={};"
    with pytest.raises(RuntimeError):
        _extract_js_object(js, "titletbl")


@pytest.mark.light
def test_extract_js_object_handles_eof_line_comment():
    """Trailing line comments without terminal newline are stripped."""
    js = 'datatbl={"k1":[0,1,2]}; // trailing comment without newline'
    parsed = _extract_js_object(js, "datatbl")
    assert parsed["k1"][1] == 1


@pytest.mark.light
def test_charset_from_content_type_extracts_charset_token():
    """Content-Type charset token is parsed correctly."""
    value = "application/javascript; charset=Shift_JIS"
    assert _charset_from_content_type(value) == "Shift_JIS"
    assert _charset_from_content_type("application/javascript") is None


@pytest.mark.light
def test_decode_textage_response_prefers_cp932_fallback():
    """Unknown encoding responses fall back to cp932 and keep Japanese text."""
    body = "titletbl={'k':['Raison d\'&ecirc;tre','～交差する宿命～']};".encode("cp932")
    response = SimpleNamespace(content=body, headers={"Content-Type": "application/javascript"})
    response.encoding = None
    decoded = _decode_textage_response(response)
    assert "交差する宿命" in decoded
