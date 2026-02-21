"""Parse BEMANIWiki title-alias conversion table."""

from __future__ import annotations

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

SPACE_RE = re.compile(r"\s+")
TARGET_HEADERS = (
    "\u6b63\u5f0f\u66f2\u540d",
    "\u7f6e\u304d\u63db\u3048\u5f8c\u306e\u66f2\u540d",
    "\u5099\u8003",
)


@dataclass(frozen=True)
class WikiAliasRow:
    """Single row parsed from BEMANIWiki conversion table."""

    official_title: str
    replaced_titles: tuple[str, ...]
    note: str


@dataclass(frozen=True)
class WikiAliasParseReport:
    """Diagnostic counters for BEMANIWiki conversion table parsing."""

    tables_scanned: int
    matched_tables: int
    selected_table_index: int
    parsed_rows_total: int
    definition_rows: int
    skipped_rows_by_reason: dict[str, int]


def _normalize_text(value: str) -> str:
    return SPACE_RE.sub(" ", value).strip()


def _read_table_headers(table) -> tuple[str, ...]:
    thead = table.find("thead")
    if thead is None:
        return tuple()
    header_row = thead.find("tr")
    if header_row is None:
        return tuple()
    headers = tuple(
        _normalize_text(cell.get_text(" ", strip=True))
        for cell in header_row.find_all(["th", "td"], recursive=False)
    )
    return headers


def _as_colspan(cell) -> int:
    raw = cell.get("colspan")
    if raw is None:
        return 1
    try:
        return int(str(raw))
    except ValueError:
        return 1


def parse_bemaniwiki_title_alias_table(
    html_text: str,
) -> tuple[list[WikiAliasRow], WikiAliasParseReport]:
    """Extract conversion definitions from the target BEMANIWiki table."""
    soup = BeautifulSoup(html_text, "html.parser")
    all_tables = soup.select("table.style_table")

    matched: list[tuple[int, object]] = []
    for index, table in enumerate(all_tables):
        if _read_table_headers(table) == TARGET_HEADERS:
            matched.append((index, table))

    if not matched:
        raise RuntimeError("BEMANIWiki conversion table not found (target header mismatch)")
    if len(matched) > 1:
        raise RuntimeError("BEMANIWiki conversion table matched multiple tables")

    table_index, table = matched[0]
    body = table.find("tbody") or table

    rows: list[WikiAliasRow] = []
    skipped: dict[str, int] = {
        "section_header": 0,
        "colspan2_special": 0,
        "missing_required_cell": 0,
        "empty_replaced_candidates": 0,
        "unexpected_structure": 0,
    }
    parsed_rows_total = 0

    for tr in body.find_all("tr", recursive=False):
        parsed_rows_total += 1
        tds = tr.find_all("td", recursive=False)
        if not tds:
            skipped["unexpected_structure"] += 1
            continue

        if len(tds) == 1 and _as_colspan(tds[0]) >= 3:
            skipped["section_header"] += 1
            continue

        if len(tds) == 2 and _as_colspan(tds[0]) == 2:
            skipped["colspan2_special"] += 1
            continue

        if len(tds) != 3:
            skipped["unexpected_structure"] += 1
            continue

        if any(_as_colspan(cell) > 1 for cell in tds):
            skipped["unexpected_structure"] += 1
            continue

        official_title = _normalize_text(tds[0].get_text(" ", strip=True))
        replaced_raw = tds[1].get_text(separator="\n")
        note = _normalize_text(tds[2].get_text(" ", strip=True))

        if not official_title or not tds[1].get_text(strip=True):
            skipped["missing_required_cell"] += 1
            continue

        replaced_titles = tuple(
            item.strip() for item in replaced_raw.splitlines() if item.strip()
        )
        if not replaced_titles:
            skipped["empty_replaced_candidates"] += 1
            continue

        rows.append(
            WikiAliasRow(
                official_title=official_title,
                replaced_titles=replaced_titles,
                note=note,
            )
        )

    return rows, WikiAliasParseReport(
        tables_scanned=len(all_tables),
        matched_tables=len(matched),
        selected_table_index=table_index,
        parsed_rows_total=parsed_rows_total,
        definition_rows=len(rows),
        skipped_rows_by_reason=skipped,
    )
