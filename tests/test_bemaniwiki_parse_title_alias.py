"""Fixture tests for BEMANIWiki title alias parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.wiki.bemaniwiki_parse_title_alias import parse_bemaniwiki_title_alias_table

FIXTURE_PATH = Path("tests/fixtures/bemaniwiki_title_alias.html")


@pytest.mark.light
def test_parse_bemaniwiki_title_alias_fixture():
    """Target table is selected and conversion rows are parsed correctly."""
    html_text = FIXTURE_PATH.read_text(encoding="utf-8")
    rows, report = parse_bemaniwiki_title_alias_table(html_text)

    titles = [row.official_title for row in rows]
    assert "ONLY_THIS_MACHINE" not in titles
    assert report.matched_tables == 1
    assert report.parsed_rows_total == 8
    assert report.definition_rows == 3
    assert report.skipped_rows_by_reason["section_header"] == 1
    assert report.skipped_rows_by_reason["colspan2_special"] == 2
    assert report.skipped_rows_by_reason["missing_required_cell"] == 2

    lamour_row = next(
        row for row in rows if row.official_title == "L'amour et la libert\u00e9"
    )
    assert lamour_row.replaced_titles == ("L'amour et la liberte", "Lamour et la liberte")
