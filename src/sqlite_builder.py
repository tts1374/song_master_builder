"""
SQLite 楽曲マスターデータベースを構築・更新するモジュール。
"""

from __future__ import annotations

import csv
import html
import os
import re
import sqlite3
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib import request as urllib_request

import requests

from src.generator.alias_seed_manual import seed_manual_aliases_from_csv
from src.generator.alias_seed_official import reset_music_title_aliases, seed_official_aliases
from src.verify.alias_verify import verify_music_title_alias_integrity

TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")

JST = timezone(timedelta(hours=9), "JST")

# 検索互換性に影響するため、置換定義はこの1箇所で管理する。
TITLE_SEARCH_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("ä", "a"),
    ("ö", "o"),
    ("ü", "u"),
    ("ß", "ss"),
    ("æ", "ae"),
    ("œ", "oe"),
    ("ø", "o"),
    ("å", "a"),
    ("ç", "c"),
    ("ñ", "n"),
    ("á", "a"),
    ("à", "a"),
    ("â", "a"),
    ("ã", "a"),
    ("é", "e"),
    ("è", "e"),
    ("ê", "e"),
    ("ë", "e"),
    ("í", "i"),
    ("ì", "i"),
    ("î", "i"),
    ("ï", "i"),
    ("ó", "o"),
    ("ò", "o"),
    ("ô", "o"),
    ("õ", "o"),
    ("ú", "u"),
    ("ù", "u"),
    ("û", "u"),
    ("ý", "y"),
    ("ÿ", "y"),
)


def normalize_textage_string(s: str) -> str:
    """Textage由来文字列を表示用に正規化する。"""
    if s is None:
        return ""

    value = str(s)
    value = html.unescape(value)
    value = TAG_RE.sub("", value)
    value = SPACE_RE.sub(" ", value).strip()
    return value


def normalize_title_search_key(title: str) -> str:
    """
    検索用タイトルキーを正規化する。

    仕様固定順序:
    1) 小文字化
    2) trim
    3) 置換テーブル
    4) NFD 分解 + 結合文字除去
    5) 連続空白圧縮
    """
    if title is None:
        value = ""
    else:
        value = str(title)

    value = value.lower()
    value = value.strip()

    for source, target in TITLE_SEARCH_REPLACEMENTS:
        value = value.replace(source, target)

    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = SPACE_RE.sub(" ", value)
    return value


def now_iso() -> str:
    """現在のJST時刻を ISO 8601 形式で返す。"""
    return datetime.now(JST).isoformat()


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO8601 with Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class InfPackRow:
    """One row in `data/inf_pack.csv`."""

    pack_code: str
    pack_name: str
    display_order: int


@dataclass(frozen=True)
class InfUnlockEntry:
    """One song unlock row parsed from official INFINITAS music page."""

    title: str
    unlock_type: str
    pack_name: str | None = None


@dataclass(frozen=True)
class InfUnlockOverrideRow:
    """One row in INF unlock override CSV."""

    textage_id: str
    unlock_type: str
    inf_pack_id: int | None


def _normalize_html_text(value: str) -> str:
    """Normalize HTML fragment into a compact display text."""
    normalized = re.sub(r"<br\s*/?>", "\n", value, flags=re.I)
    normalized = TAG_RE.sub("", normalized)
    normalized = html.unescape(normalized)
    normalized = normalized.replace("\u3000", " ")
    normalized = SPACE_RE.sub(" ", normalized)
    return normalized.strip()


def _extract_section_html_until_next_cat(after_html: str) -> str:
    """Return one section block from current cat to just before the next cat."""
    next_cat_match = _INF_CAT_DIV_OPEN_RE.search(after_html)
    if next_cat_match is None:
        return after_html
    return after_html[: next_cat_match.start()]


def _extract_first_table_html(after_html: str) -> str | None:
    section_html = _extract_section_html_until_next_cat(after_html)
    table_match = re.search(r"<table>(.*?)</table>", section_html, re.S)
    if table_match is None:
        return None
    return table_match.group(1)


def _extract_titles_from_table_html(table_html: str) -> list[str]:
    titles: list[str] = []
    for row_html in _INF_TR_RE.findall(table_html):
        if "<td" not in row_html:
            continue
        title_cell = re.search(r"<td[^>]*>(.*?)</td>", row_html, re.S)
        if title_cell is None:
            continue
        title = _normalize_html_text(title_cell.group(1))
        if title:
            titles.append(title)
    return titles


def _normalize_inf_pack_name(label: str) -> str:
    normalized = label.strip()
    normalized = re.sub(r"^beatmania\s+IIDX\s+INFINITAS\s+", "", normalized)
    normalized = SPACE_RE.sub(" ", normalized).strip()
    normalized = _INF_PACK_LABEL_SPACE_RE.sub(r"\1(", normalized)
    return normalized


def fetch_inf_music_index_html(
    inf_music_index_url: str | None = None,
    timeout_sec: int = 30,
) -> str:
    """Fetch official INFINITAS music page HTML."""
    resolved_url = inf_music_index_url or DEFAULT_INF_MUSIC_INDEX_URL

    try:
        with urllib_request.urlopen(resolved_url, timeout=timeout_sec) as response:
            raw = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
    except OSError as exc:
        raise RuntimeError(
            f"failed to fetch INFINITAS music page: {resolved_url}"
        ) from exc

    try:
        return raw.decode(charset, errors="strict")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def parse_inf_unlock_entries_from_music_index_html(page_html: str) -> list[InfUnlockEntry]:
    """Parse unlock type and pack mapping from official INFINITAS page HTML."""
    parsed_entries: list[InfUnlockEntry] = []
    observed_required_sections: set[str] = set()

    for cat_match in _INF_TARGET_DIV_RE.finditer(page_html):
        section_id = (cat_match.group(1) or "").strip()
        cat_inner_html = cat_match.group(2)
        after_html = page_html[cat_match.end() :]
        section_html = _extract_section_html_until_next_cat(after_html)
        table_html = _extract_first_table_html(after_html)

        heading_text = _normalize_html_text(cat_inner_html)
        section_text = _normalize_html_text(section_html)

        unlock_type = _INF_MUSIC_SECTION_ID_TO_UNLOCK_TYPE.get(section_id)
        if unlock_type is None and section_id == _INF_NEWSONG_SECTION_ID:
            # newsong は現行ページで BIT 解禁曲の先頭セクションとして掲載される。
            if "BIT解禁曲" in heading_text or "BIT解禁曲" in section_text:
                unlock_type = INF_UNLOCK_TYPE_BIT
        if (
            unlock_type is None
            and not section_id
            and table_html is not None
            and "BIT解禁曲" in heading_text
        ):
            # 現行ページでは newsong 配下の2つ目 cat(idなし) に BIT 文言と table がぶら下がる。
            unlock_type = INF_UNLOCK_TYPE_BIT

        if unlock_type is not None:
            if section_id in _INF_REQUIRED_SECTION_IDS:
                observed_required_sections.add(section_id)
                if table_html is None:
                    raise RuntimeError(
                        f"official INFINITAS page section has no table: id={section_id}"
                    )
            if table_html is None:
                continue
            for title in _extract_titles_from_table_html(table_html):
                parsed_entries.append(
                    InfUnlockEntry(
                        title=title,
                        unlock_type=unlock_type,
                    )
                )
            continue

        if table_html is None:
            continue

        strong_match = re.search(r"<strong>(.*?)</strong>", cat_inner_html, re.S)
        if strong_match is None:
            continue

        pack_heading = _normalize_html_text(strong_match.group(1))
        if "楽曲パック" not in pack_heading:
            continue

        pack_name = _normalize_inf_pack_name(pack_heading)
        if not pack_name:
            raise RuntimeError(
                "failed to parse pack name from official INFINITAS section: "
                f"id={section_id}"
            )

        for title in _extract_titles_from_table_html(table_html):
            parsed_entries.append(
                InfUnlockEntry(
                    title=title,
                    unlock_type=INF_UNLOCK_TYPE_PACK,
                    pack_name=pack_name,
                )
            )

    missing_sections = set(_INF_REQUIRED_SECTION_IDS) - observed_required_sections
    if missing_sections:
        missing = ", ".join(sorted(missing_sections))
        raise RuntimeError(
            "official INFINITAS page does not contain required sections: "
            f"{missing}"
        )

    if not parsed_entries:
        raise RuntimeError("official INFINITAS page parsing produced no unlock entries")

    return parsed_entries


def load_inf_pack_rows_from_csv(
    inf_pack_csv_path: str | None = None,
) -> list[InfPackRow]:
    """Load `data/inf_pack.csv` rows as authoritative pack definitions."""
    resolved_csv_path = inf_pack_csv_path or DEFAULT_INF_PACK_CSV_PATH

    try:
        with open(resolved_csv_path, "r", encoding="utf-8", newline="") as file_obj:
            reader = csv.DictReader(file_obj)
            expected_columns = ["pack_code", "pack_name", "display_order"]
            if reader.fieldnames != expected_columns:
                raise RuntimeError(
                    "inf_pack CSV columns must be exactly "
                    f"{expected_columns}: {resolved_csv_path}"
                )

            rows: list[InfPackRow] = []
            seen_pack_codes: set[str] = set()
            for line_no, row in enumerate(reader, start=2):
                pack_code = str(row.get("pack_code", "")).strip()
                pack_name = str(row.get("pack_name", "")).strip()
                display_order_raw = str(row.get("display_order", "")).strip()

                if not pack_code:
                    raise RuntimeError(f"inf_pack CSV has empty pack_code at line {line_no}")
                if not pack_name:
                    raise RuntimeError(f"inf_pack CSV has empty pack_name at line {line_no}")
                if pack_code in seen_pack_codes:
                    raise RuntimeError(
                        f"inf_pack CSV has duplicate pack_code '{pack_code}' at line {line_no}"
                    )

                try:
                    display_order = int(display_order_raw)
                except ValueError as exc:
                    raise RuntimeError(
                        f"inf_pack CSV has invalid display_order at line {line_no}: {display_order_raw}"
                    ) from exc

                rows.append(
                    InfPackRow(
                        pack_code=pack_code,
                        pack_name=pack_name,
                        display_order=display_order,
                    )
                )
                seen_pack_codes.add(pack_code)
    except OSError as exc:
        raise RuntimeError(f"failed to load inf_pack CSV: {resolved_csv_path}") from exc

    if not rows:
        raise RuntimeError(f"inf_pack CSV has no data rows: {resolved_csv_path}")

    return rows


def seed_inf_pack_table(
    conn: sqlite3.Connection,
    inf_pack_csv_path: str | None = None,
) -> dict:
    """Seed `inf_pack` table from authoritative `data/inf_pack.csv`."""
    inf_pack_rows = load_inf_pack_rows_from_csv(inf_pack_csv_path)
    cur = conn.cursor()
    now = now_utc_iso()

    for row in inf_pack_rows:
        cur.execute(
            """
            INSERT INTO inf_pack (
                pack_code,
                pack_name,
                display_order,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(pack_code) DO UPDATE SET
                pack_name = excluded.pack_name,
                display_order = excluded.display_order,
                updated_at = excluded.updated_at
            """,
            (
                row.pack_code,
                row.pack_name,
                row.display_order,
                now,
                now,
            ),
        )

    pack_codes = [row.pack_code for row in inf_pack_rows]
    placeholders = ",".join(["?"] * len(pack_codes))
    now = now_iso()
    cur.execute(
        f"""
        UPDATE music
        SET inf_pack_id = NULL,
            inf_unlock_type = CASE
                WHEN inf_unlock_type = 'pack' THEN NULL
                ELSE inf_unlock_type
            END,
            updated_at = ?
        WHERE inf_pack_id IN (
            SELECT inf_pack_id
            FROM inf_pack
            WHERE pack_code NOT IN ({placeholders})
        )
        """,
        [now, *pack_codes],
    )
    cur.execute(
        f"DELETE FROM inf_pack WHERE pack_code NOT IN ({placeholders})",
        pack_codes,
    )

    cur.execute("SELECT COUNT(*) FROM inf_pack;")
    inf_pack_count = int(cur.fetchone()[0])
    return {
        "csv_row_count": len(inf_pack_rows),
        "db_row_count": inf_pack_count,
    }


def _load_inf_alias_map(conn: sqlite3.Connection) -> dict[str, str]:
    """Load alias_scope='inf' exact match map (`alias -> textage_id`)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT alias, textage_id
        FROM music_title_alias
        WHERE alias_scope = 'inf'
          AND alias_type IN ('official', 'manual')
        """
    )
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError(
            "music_title_alias alias_scope='inf' with alias_type in (official, manual) has no rows"
        )
    return {str(alias): str(textage_id) for alias, textage_id in rows}


def _load_inf_unlock_overrides_from_csv(
    conn: sqlite3.Connection,
    inf_unlock_override_csv_path: str | None = None,
) -> list[InfUnlockOverrideRow]:
    """Load optional INF unlock overrides from CSV."""
    resolved_csv_path = (
        inf_unlock_override_csv_path or DEFAULT_INF_UNLOCK_OVERRIDE_CSV_PATH
    )
    if not resolved_csv_path or not os.path.exists(resolved_csv_path):
        return []

    cur = conn.cursor()
    cur.execute("SELECT textage_id FROM music")
    existing_textage_ids = {str(row[0]) for row in cur.fetchall()}

    rows: list[InfUnlockOverrideRow] = []
    seen_textage_ids: set[str] = set()
    try:
        with open(resolved_csv_path, "r", encoding="utf-8", newline="") as file_obj:
            reader = csv.DictReader(file_obj)
            expected_columns = ["textage_id", "inf_unlock_type", "inf_pack_id"]
            if reader.fieldnames != expected_columns:
                raise RuntimeError(
                    "inf unlock override CSV columns must be exactly "
                    f"{expected_columns}: {resolved_csv_path}"
                )

            for line_no, row in enumerate(reader, start=2):
                textage_id = str(row.get("textage_id", "")).strip()
                unlock_type = str(row.get("inf_unlock_type", "")).strip()
                inf_pack_id_raw = str(row.get("inf_pack_id", "")).strip()

                if not textage_id:
                    raise RuntimeError(
                        f"inf unlock override CSV has empty textage_id at line {line_no}"
                    )
                if textage_id not in existing_textage_ids:
                    raise RuntimeError(
                        "inf unlock override CSV has textage_id not found in music: "
                        f"line={line_no} textage_id={textage_id}"
                    )
                if textage_id in seen_textage_ids:
                    raise RuntimeError(
                        "inf unlock override CSV has duplicate textage_id: "
                        f"line={line_no} textage_id={textage_id}"
                    )
                if unlock_type not in INF_UNLOCK_TYPES:
                    raise RuntimeError(
                        "inf unlock override CSV has invalid inf_unlock_type: "
                        f"line={line_no} value={unlock_type!r}"
                    )

                inf_pack_id: int | None = None
                if inf_pack_id_raw:
                    try:
                        inf_pack_id = int(inf_pack_id_raw)
                    except ValueError as exc:
                        raise RuntimeError(
                            "inf unlock override CSV has invalid inf_pack_id: "
                            f"line={line_no} value={inf_pack_id_raw!r}"
                        ) from exc

                if unlock_type == INF_UNLOCK_TYPE_PACK and inf_pack_id is None:
                    raise RuntimeError(
                        "inf unlock override CSV requires inf_pack_id when inf_unlock_type='pack': "
                        f"line={line_no}"
                    )
                if unlock_type in (
                    INF_UNLOCK_TYPE_INITIAL,
                    INF_UNLOCK_TYPE_DJP,
                    INF_UNLOCK_TYPE_BIT,
                ) and inf_pack_id is not None:
                    raise RuntimeError(
                        "inf unlock override CSV must not set inf_pack_id for non-pack types: "
                        f"line={line_no}"
                    )

                rows.append(
                    InfUnlockOverrideRow(
                        textage_id=textage_id,
                        unlock_type=unlock_type,
                        inf_pack_id=inf_pack_id,
                    )
                )
                seen_textage_ids.add(textage_id)
    except OSError as exc:
        raise RuntimeError(
            f"failed to load inf unlock override CSV: {resolved_csv_path}"
        ) from exc

    return rows


def _validate_inf_unlock_integrity(conn: sqlite3.Connection) -> None:
    """Validate INF unlock constraints at application layer."""
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM music
        WHERE is_inf_active = 0
          AND (inf_unlock_type IS NOT NULL OR inf_pack_id IS NOT NULL)
        """
    )
    inactive_with_unlock = int(cur.fetchone()[0])
    if inactive_with_unlock > 0:
        raise RuntimeError(
            "is_inf_active=0 rows must not have inf_unlock_type/inf_pack_id: "
            f"{inactive_with_unlock}"
        )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM music
        WHERE inf_unlock_type = 'pack'
          AND inf_pack_id IS NULL
        """
    )
    pack_without_pack_id = int(cur.fetchone()[0])
    if pack_without_pack_id > 0:
        raise RuntimeError(
            "inf_unlock_type='pack' rows must have inf_pack_id: "
            f"{pack_without_pack_id}"
        )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM music
        WHERE inf_unlock_type IN ('initial', 'djp', 'bit')
          AND inf_pack_id IS NOT NULL
        """
    )
    non_pack_with_pack_id = int(cur.fetchone()[0])
    if non_pack_with_pack_id > 0:
        raise RuntimeError(
            "inf_unlock_type in ('initial','djp','bit') rows must have inf_pack_id NULL: "
            f"{non_pack_with_pack_id}"
        )


def apply_inf_unlock_information(
    conn: sqlite3.Connection,
    inf_music_index_url: str | None = None,
    inf_pack_csv_path: str | None = None,
    inf_unlock_override_csv_path: str | None = None,
) -> dict:
    """Apply INF unlock type/pack info to `music` from official and CSV authorities."""
    resolved_url = inf_music_index_url or DEFAULT_INF_MUSIC_INDEX_URL
    page_html = fetch_inf_music_index_html(resolved_url)
    unlock_entries = parse_inf_unlock_entries_from_music_index_html(page_html)
    alias_map = _load_inf_alias_map(conn)

    cur = conn.cursor()
    now = now_iso()

    cur.execute(
        """
        UPDATE music
        SET inf_unlock_type = NULL,
            inf_pack_id = NULL,
            updated_at = ?
        WHERE inf_unlock_type IS NOT NULL
           OR inf_pack_id IS NOT NULL
        """,
        (now,),
    )

    inf_pack_seed_report = seed_inf_pack_table(
        conn=conn,
        inf_pack_csv_path=inf_pack_csv_path,
    )

    cur.execute(
        """
        SELECT inf_pack_id, pack_code, pack_name
        FROM inf_pack
        """
    )
    pack_rows = cur.fetchall()

    pack_name_to_row: dict[str, tuple[int, str]] = {}
    duplicate_pack_names: set[str] = set()
    for inf_pack_id, pack_code, pack_name in pack_rows:
        normalized_name = str(pack_name)
        if normalized_name in pack_name_to_row:
            duplicate_pack_names.add(normalized_name)
            continue
        pack_name_to_row[normalized_name] = (int(inf_pack_id), str(pack_code))

    assignments_by_textage_id: dict[str, tuple[str, int | None]] = {}
    unmatched_titles: Counter[str] = Counter()
    unresolved_pack_names: Counter[str] = Counter()
    ambiguous_pack_names: Counter[str] = Counter()
    conflicting_unlock_assignments: list[tuple[str, tuple[str, int | None], tuple[str, int | None]]] = []

    for entry in unlock_entries:
        textage_id = alias_map.get(entry.title)
        if textage_id is None:
            unmatched_titles[entry.title] += 1
            continue

        resolved_pack_id: int | None = None
        if entry.unlock_type == INF_UNLOCK_TYPE_PACK:
            assert entry.pack_name is not None
            if entry.pack_name in duplicate_pack_names:
                ambiguous_pack_names[entry.pack_name] += 1
                continue

            pack_row = pack_name_to_row.get(entry.pack_name)
            if pack_row is None:
                unresolved_pack_names[entry.pack_name] += 1
                continue
            resolved_pack_id = pack_row[0]

        next_assignment = (entry.unlock_type, resolved_pack_id)
        previous_assignment = assignments_by_textage_id.get(textage_id)
        if previous_assignment is not None and previous_assignment != next_assignment:
            conflicting_unlock_assignments.append(
                (textage_id, previous_assignment, next_assignment)
            )
            continue

        assignments_by_textage_id[textage_id] = next_assignment

    if conflicting_unlock_assignments:
        sample = ", ".join(
            [
                f"{textage_id}:{prev}->{next_value}"
                for textage_id, prev, next_value in conflicting_unlock_assignments[:10]
            ]
        )
        raise RuntimeError(
            "official INFINITAS unlock assignments conflict on same textage_id: "
            f"{sample}"
        )

    override_rows = _load_inf_unlock_overrides_from_csv(
        conn=conn,
        inf_unlock_override_csv_path=inf_unlock_override_csv_path,
    )
    if override_rows:
        valid_inf_pack_ids = {int(row[0]) for row in pack_rows}
        unknown_override_pack_ids = sorted(
            {
                int(row.inf_pack_id)
                for row in override_rows
                if row.inf_pack_id is not None and int(row.inf_pack_id) not in valid_inf_pack_ids
            }
        )
        if unknown_override_pack_ids:
            raise RuntimeError(
                "inf unlock override CSV references unknown inf_pack_id values: "
                f"{unknown_override_pack_ids}"
            )

        for row in override_rows:
            assignments_by_textage_id[row.textage_id] = (row.unlock_type, row.inf_pack_id)

    updated_music_rows = 0
    skipped_non_inf_active_rows = 0
    for textage_id, (unlock_type, inf_pack_id) in assignments_by_textage_id.items():
        cur.execute(
            """
            UPDATE music
            SET inf_unlock_type = ?,
                inf_pack_id = ?,
                updated_at = ?
            WHERE textage_id = ?
              AND is_inf_active = 1
            """,
            (unlock_type, inf_pack_id, now_iso(), textage_id),
        )
        if cur.rowcount > 0:
            updated_music_rows += int(cur.rowcount)
        else:
            skipped_non_inf_active_rows += 1

    _validate_inf_unlock_integrity(conn)

    return {
        "source_url": resolved_url,
        "parsed_entry_count": len(unlock_entries),
        "assigned_textage_count": len(assignments_by_textage_id),
        "updated_music_rows": updated_music_rows,
        "skipped_non_inf_active_rows": skipped_non_inf_active_rows,
        "unmatched_title_count": int(sum(unmatched_titles.values())),
        "unmatched_titles_top10": [
            {"title": title, "count": count}
            for title, count in sorted(
                unmatched_titles.items(),
                key=lambda item: (-item[1], item[0]),
            )[:10]
        ],
        "unresolved_pack_name_count": int(sum(unresolved_pack_names.values())),
        "unresolved_pack_names_top10": [
            {"pack_name": pack_name, "count": count}
            for pack_name, count in sorted(
                unresolved_pack_names.items(),
                key=lambda item: (-item[1], item[0]),
            )[:10]
        ],
        "ambiguous_pack_name_count": int(sum(ambiguous_pack_names.values())),
        "ambiguous_pack_names_top10": [
            {"pack_name": pack_name, "count": count}
            for pack_name, count in sorted(
                ambiguous_pack_names.items(),
                key=lambda item: (-item[1], item[0]),
            )[:10]
        ],
        "override_row_count": len(override_rows),
        "inf_pack_seed": inf_pack_seed_report,
    }


CHART_TYPES = [
    # (type, play_style, difficulty, act_index)
    (1, "SP", "BEGINNER", 3),
    (2, "SP", "NORMAL", 5),
    (3, "SP", "HYPER", 7),
    (4, "SP", "ANOTHER", 9),
    (5, "SP", "LEGGENDARIA", 11),
    (7, "DP", "NORMAL", 15),
    (8, "DP", "HYPER", 17),
    (9, "DP", "ANOTHER", 19),
    (10, "DP", "LEGGENDARIA", 21),
]
ACTBL_TITLE_QUALIFIER_INDEX = 23
SONG_FLAG_AC = 0x01
SONG_FLAG_INF = 0x02
SONG_FLAG_INF_BEGINNER = 0x04
SONG_FLAG_INF_LEGGENDARIA = 0x08
CHART_OPT_AC_AVAILABLE = 0x04
LEGGENDARIA_CHART_TYPES = {5, 10}
DEFAULT_MANUAL_ALIAS_AC_CSV_PATH = "data/music_alias_manual_ac.csv"
DEFAULT_MANUAL_ALIAS_INF_CSV_PATH = "data/music_alias_manual_inf.csv"
DEFAULT_INF_MANUAL_ALIAS_PATCH_CSV_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "music_alias_manual_inf_patch.csv",
    )
)
DEFAULT_INF_PACK_CSV_PATH = "data/inf_pack.csv"
DEFAULT_INF_UNLOCK_OVERRIDE_CSV_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "inf_unlock_override.csv",
    )
)
DEFAULT_INF_MUSIC_INDEX_URL = "https://p.eagate.573.jp/game/infinitas/2/music/index.html"
INF_UNLOCK_TYPE_INITIAL = "initial"
INF_UNLOCK_TYPE_DJP = "djp"
INF_UNLOCK_TYPE_BIT = "bit"
INF_UNLOCK_TYPE_PACK = "pack"
INF_UNLOCK_TYPES = (
    INF_UNLOCK_TYPE_INITIAL,
    INF_UNLOCK_TYPE_DJP,
    INF_UNLOCK_TYPE_BIT,
    INF_UNLOCK_TYPE_PACK,
)
_INF_MUSIC_SECTION_ID_TO_UNLOCK_TYPE = {
    "default": INF_UNLOCK_TYPE_INITIAL,
    "djp": INF_UNLOCK_TYPE_DJP,
    "bit": INF_UNLOCK_TYPE_BIT,
}
_INF_NEWSONG_SECTION_ID = "newsong"
_INF_PACK_SECTION_ID_PREFIX = "pac_"
_INF_REQUIRED_SECTION_IDS = tuple(_INF_MUSIC_SECTION_ID_TO_UNLOCK_TYPE.keys())
_INF_CAT_DIV_OPEN_RE = re.compile(
    r'<div class="cat"(?:\s+id="[^"]+")?\s*>',
    re.S,
)
_INF_TARGET_DIV_RE = re.compile(
    r'<div class="cat"(?:\s+id="([^"]+)")?\s*>(.*?)</div>',
    re.S,
)
_INF_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_INF_PACK_LABEL_SPACE_RE = re.compile(r"(楽曲パック\s+vol\.\d+)\s+\(")
# Backward compatibility for existing callers/tests that still import this name.
DEFAULT_MANUAL_ALIAS_CSV_PATH = DEFAULT_MANUAL_ALIAS_AC_CSV_PATH


def _parse_textage_hex_or_int(value: object) -> int:
    """Parse Textage value that may be int or base16 token string."""
    if isinstance(value, int):
        return value
    return int(str(value), 16)


def _resolve_chart_scope_activity(
    *,
    song_flags: int,
    chart_type: int,
    level: int,
    chart_opt: int,
) -> tuple[int, int]:
    """
    Resolve AC/INF chart activity by Textage's scrlist.js logic.

    - `level <= 0` is always inactive for both scopes.
    - AC chart requires song AC flag and chart AC-available flag.
    - INF chart follows song/option gates, plus BEGINNER/LEGGENDARIA scope gates.
    """
    if level <= 0:
        return 0, 0

    is_ac_active = (
        1
        if (song_flags & SONG_FLAG_AC) and (chart_opt & CHART_OPT_AC_AVAILABLE)
        else 0
    )

    is_inf_active = 0
    if (song_flags & SONG_FLAG_INF) != 0:
        if (chart_opt & CHART_OPT_AC_AVAILABLE) != 0 or (
            song_flags & SONG_FLAG_INF_LEGGENDARIA
        ) != 0:
            if chart_type > 1 or (song_flags & SONG_FLAG_INF_BEGINNER) != 0:
                if (
                    chart_type not in LEGGENDARIA_CHART_TYPES
                    or (song_flags & SONG_FLAG_INF_LEGGENDARIA) != 0
                ):
                    is_inf_active = 1

    return is_ac_active, is_inf_active


def download_latest_sqlite_from_release(
    owner: str,
    repo: str,
    sqlite_path: str,
    token: str | None = None,
    asset_name: str = "song_master.sqlite",
) -> dict:
    """
    最新リリースから指定名のアセットをダウンロードする。
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"

    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 404:
        return {"downloaded": False, "asset_updated_at": None}
    response.raise_for_status()
    release = response.json()

    target = None
    for asset in release.get("assets", []):
        if asset.get("name") == asset_name:
            target = asset
            break

    if not target or not target.get("browser_download_url"):
        return {"downloaded": False, "asset_updated_at": None}

    download_headers = {}
    if token:
        download_headers["Authorization"] = f"Bearer {token}"

    asset_response = requests.get(
        target["browser_download_url"], headers=download_headers, timeout=60
    )
    asset_response.raise_for_status()

    os.makedirs(os.path.dirname(sqlite_path) or ".", exist_ok=True)
    with open(sqlite_path, "wb") as file_obj:
        file_obj.write(asset_response.content)

    return {"downloaded": True, "asset_updated_at": target.get("updated_at")}


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name});")
    rows = cur.fetchall()
    return any(row[1] == column_name for row in rows)


def _backfill_title_search_keys(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT music_id, title, title_search_key FROM music;")

    updates: list[tuple[str, int]] = []
    for music_id, title, title_search_key in cur.fetchall():
        if title_search_key is None or title_search_key == "":
            updates.append((normalize_title_search_key(title), music_id))

    if updates:
        cur.executemany(
            "UPDATE music SET title_search_key = ? WHERE music_id = ?",
            updates,
        )


def _extract_actbl_title_qualifier(act_row: object) -> str:
    """Extract explicit display qualifier from actbl row when present."""
    if not isinstance(act_row, list):
        return ""
    if len(act_row) <= ACTBL_TITLE_QUALIFIER_INDEX:
        return ""

    raw_value = act_row[ACTBL_TITLE_QUALIFIER_INDEX]
    if not isinstance(raw_value, str):
        return ""

    qualifier = normalize_textage_string(raw_value)
    return qualifier


# pylint: disable-next=too-many-locals
def resolve_music_title_qualifiers(
    conn: sqlite3.Connection,
    explicit_title_qualifier_by_textage_id: dict[str, str] | None = None,
):
    """
    Resolve display-only title qualifiers by these rules:
    1) explicit actbl qualifier wins
    2) if duplicate title and no explicit qualifier, fill (AC)/(INF) for single-scope actives
    3) otherwise empty
    """
    explicit_map = explicit_title_qualifier_by_textage_id or {}
    cur = conn.cursor()
    cur.execute(
        """
        SELECT music_id, textage_id, title, is_ac_active, is_inf_active, title_qualifier
        FROM music
        ORDER BY music_id;
        """
    )
    rows = cur.fetchall()
    title_counts = Counter(str(row[2]) for row in rows)

    updates: list[tuple[str, int]] = []
    for music_id, textage_id, title, is_ac_active, is_inf_active, current_qualifier in rows:
        textage_key = str(textage_id)
        explicit_qualifier = explicit_map.get(textage_key, "")
        if explicit_qualifier:
            resolved = explicit_qualifier
        elif title_counts[str(title)] > 1:
            ac = int(is_ac_active)
            inf = int(is_inf_active)
            if ac == 1 and inf == 0:
                resolved = "(AC)"
            elif ac == 0 and inf == 1:
                resolved = "(INF)"
            else:
                resolved = ""
        else:
            resolved = ""

        if str(current_qualifier or "") != resolved:
            updates.append((resolved, int(music_id)))

    if updates:
        cur.executemany(
            "UPDATE music SET title_qualifier = ? WHERE music_id = ?",
            updates,
        )


def ensure_schema(conn: sqlite3.Connection):
    """DBスキーマの作成・移行を行う。"""
    cur = conn.cursor()

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS inf_pack (
        inf_pack_id INTEGER PRIMARY KEY AUTOINCREMENT,
        pack_code TEXT NOT NULL UNIQUE,
        pack_name TEXT NOT NULL,
        display_order INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS music (
        music_id INTEGER PRIMARY KEY AUTOINCREMENT,
        textage_id TEXT NOT NULL UNIQUE,
        version TEXT NOT NULL,
        title TEXT NOT NULL,
        title_qualifier TEXT NOT NULL DEFAULT '',
        title_search_key TEXT NOT NULL,
        artist TEXT NOT NULL,
        genre TEXT NOT NULL,
        is_ac_active INTEGER NOT NULL,
        is_inf_active INTEGER NOT NULL,
        inf_unlock_type TEXT NULL CHECK(inf_unlock_type IN ('initial', 'djp', 'bit', 'pack')),
        inf_pack_id INTEGER NULL,
        last_seen_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(inf_pack_id) REFERENCES inf_pack(inf_pack_id)
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS chart (
        chart_id INTEGER PRIMARY KEY AUTOINCREMENT,
        music_id INTEGER NOT NULL,
        play_style TEXT NOT NULL,
        difficulty TEXT NOT NULL,
        level INTEGER NOT NULL,
        notes INTEGER NOT NULL,
        is_active INTEGER NOT NULL,
        is_ac_active INTEGER NOT NULL,
        is_inf_active INTEGER NOT NULL,
        last_seen_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(music_id, play_style, difficulty),
        FOREIGN KEY(music_id) REFERENCES music(music_id)
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS meta (
        schema_version TEXT NOT NULL,
        asset_updated_at TEXT NOT NULL,
        generated_at TEXT NOT NULL
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS music_title_alias (
        alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
        textage_id TEXT NOT NULL,
        alias_scope TEXT NOT NULL CHECK(alias_scope IN ('ac', 'inf')),
        alias TEXT NOT NULL,
        alias_type TEXT NOT NULL CHECK(alias_type IN ('official', 'manual')),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(textage_id) REFERENCES music(textage_id)
    );
    """
    )

    if _table_exists(conn, "music") and not _column_exists(
        conn, "music", "title_search_key"
    ):
        cur.execute(
            "ALTER TABLE music ADD COLUMN title_search_key TEXT NOT NULL DEFAULT ''"
        )

    if _table_exists(conn, "music") and not _column_exists(
        conn, "music", "title_qualifier"
    ):
        cur.execute(
            "ALTER TABLE music ADD COLUMN title_qualifier TEXT NOT NULL DEFAULT ''"
        )

    if _table_exists(conn, "music") and not _column_exists(
        conn, "music", "inf_unlock_type"
    ):
        cur.execute(
            "ALTER TABLE music ADD COLUMN inf_unlock_type TEXT "
            "CHECK(inf_unlock_type IN ('initial', 'djp', 'bit', 'pack'))"
        )

    if _table_exists(conn, "music") and not _column_exists(
        conn, "music", "inf_pack_id"
    ):
        cur.execute(
            "ALTER TABLE music ADD COLUMN inf_pack_id INTEGER"
        )

    if _table_exists(conn, "music_title_alias") and not _column_exists(
        conn, "music_title_alias", "alias_scope"
    ):
        cur.execute(
            "ALTER TABLE music_title_alias ADD COLUMN alias_scope TEXT NOT NULL DEFAULT 'ac'"
        )

    if _table_exists(conn, "chart") and not _column_exists(conn, "chart", "is_ac_active"):
        cur.execute("ALTER TABLE chart ADD COLUMN is_ac_active INTEGER NOT NULL DEFAULT 0")

    if _table_exists(conn, "chart") and not _column_exists(conn, "chart", "is_inf_active"):
        cur.execute("ALTER TABLE chart ADD COLUMN is_inf_active INTEGER NOT NULL DEFAULT 0")

    _backfill_title_search_keys(conn)

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_chart_music_active ON chart(music_id, is_active);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_chart_filter "
        "ON chart(play_style, difficulty, level, is_active);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_chart_notes_active ON chart(is_active, notes);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_title_search_key "
        "ON music(title_search_key);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_inf_pack_id ON music(inf_pack_id);"
    )
    cur.execute("DROP INDEX IF EXISTS uq_music_title_alias_alias;")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_title_alias_textage_id "
        "ON music_title_alias(textage_id);"
    )
    cur.execute("DROP INDEX IF EXISTS uq_music_title_alias_textage_alias;")
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_music_title_alias_scope_alias "
        "ON music_title_alias(alias_scope, alias);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_title_alias_scope_alias "
        "ON music_title_alias(alias_scope, alias);"
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_music_title_alias_textage_scope_alias "
        "ON music_title_alias(textage_id, alias_scope, alias);"
    )

    conn.commit()

def upsert_meta(
    conn: sqlite3.Connection,
    schema_version: str,
    asset_updated_at: str,
    generated_at: str,
):
    """meta テーブルを最新1行で更新する。"""
    cur = conn.cursor()
    cur.execute("DELETE FROM meta;")
    cur.execute(
        "INSERT INTO meta (schema_version, asset_updated_at, generated_at) VALUES (?, ?, ?)",
        (schema_version, asset_updated_at, generated_at),
    )
    conn.commit()


def reset_all_music_active_flags(conn: sqlite3.Connection):
    """取り込み前に収録フラグを全件リセットする。"""
    cur = conn.cursor()
    now = now_iso()

    cur.execute(
        """
    UPDATE music SET
        is_ac_active = 0,
        is_inf_active = 0,
        inf_unlock_type = NULL,
        inf_pack_id = NULL,
        updated_at = ?
    """,
        (now,),
    )

    conn.commit()


def rebuild_music_title_aliases(
    conn: sqlite3.Connection,
    manual_alias_csv_path: str | None = DEFAULT_MANUAL_ALIAS_CSV_PATH,
    manual_alias_csv_paths: list[str] | tuple[str, ...] | None = None,
) -> dict:
    """Rebuild `music_title_alias` from official titles and repository-managed manual CSV(s)."""
    alias_timestamp = now_utc_iso()
    reset_music_title_aliases(conn)
    official_count = seed_official_aliases(conn, alias_timestamp)
    resolved_manual_alias_csv_paths: list[str] = []
    if manual_alias_csv_paths is not None:
        resolved_manual_alias_csv_paths = [
            str(path).strip() for path in manual_alias_csv_paths if str(path).strip()
        ]
    elif manual_alias_csv_path is not None:
        single_path = str(manual_alias_csv_path).strip()
        if single_path:
            resolved_manual_alias_csv_paths = [single_path]

    bundled_manual_alias_csv_paths = [
        DEFAULT_INF_MANUAL_ALIAS_PATCH_CSV_PATH,
    ]
    for bundled_path in bundled_manual_alias_csv_paths:
        if not os.path.exists(bundled_path):
            continue
        if bundled_path in resolved_manual_alias_csv_paths:
            continue
        resolved_manual_alias_csv_paths.append(bundled_path)

    inserted_manual_alias_count = 0
    skipped_redundant_manual_alias_count = 0

    if resolved_manual_alias_csv_paths:
        for manual_csv_path in resolved_manual_alias_csv_paths:
            manual_report = seed_manual_aliases_from_csv(
                conn=conn,
                csv_path=manual_csv_path,
                now_utc_iso=alias_timestamp,
            )
            inserted_manual_alias_count += manual_report.inserted_manual_alias_count
            skipped_redundant_manual_alias_count += (
                manual_report.skipped_redundant_manual_alias_count
            )
            print(
                "[alias/manual] inserted_manual_alias_count="
                f"{manual_report.inserted_manual_alias_count} "
                "skipped_redundant_manual_alias_count="
                f"{manual_report.skipped_redundant_manual_alias_count} "
                f"path={manual_csv_path}"
            )
    else:
        print("[alias/manual] skipped (manual_alias_csv_paths is empty)")

    verify_summary = verify_music_title_alias_integrity(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT alias_scope, alias_type, COUNT(*) AS c
        FROM music_title_alias
        GROUP BY alias_scope, alias_type
        ORDER BY alias_scope, alias_type;
        """
    )
    scope_type_counts = {
        f"{row[0]}:{row[1]}": int(row[2])
        for row in cur.fetchall()
    }
    print(f"[alias] scope_type_counts={scope_type_counts}")

    return {
        "official_alias_count": official_count,
        "inserted_manual_alias_count": inserted_manual_alias_count,
        "skipped_redundant_manual_alias_count": skipped_redundant_manual_alias_count,
        "alias_ac_music_count": verify_summary.active_ac_music_count,
        "alias_inf_music_count": verify_summary.active_inf_music_count,
        "alias_official_ac_count": verify_summary.official_ac_alias_count,
        "alias_official_inf_count": verify_summary.official_inf_alias_count,
        "scope_type_counts": scope_type_counts,
    }


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def upsert_music(
    conn: sqlite3.Connection,
    textage_id: str,
    version: str,
    title: str,
    artist: str,
    genre: str,
    is_ac_active: int,
    is_inf_active: int,
) -> int:
    """music 1件を Upsert する。"""
    cur = conn.cursor()
    now = now_iso()
    title_search_key = normalize_title_search_key(title)

    cur.execute("SELECT music_id FROM music WHERE textage_id = ?", (textage_id,))
    row = cur.fetchone()

    if row is None:
        cur.execute(
            """
        INSERT INTO music (
            textage_id, version, title, title_search_key, artist, genre,
            is_ac_active, is_inf_active,
            last_seen_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                textage_id,
                version,
                title,
                title_search_key,
                artist,
                genre,
                is_ac_active,
                is_inf_active,
                now,
                now,
                now,
            ),
        )
        return cur.lastrowid

    music_id = row[0]
    cur.execute(
        """
    UPDATE music SET
        version = ?,
        title = ?,
        title_search_key = ?,
        artist = ?,
        genre = ?,
        is_ac_active = ?,
        is_inf_active = ?,
        last_seen_at = ?,
        updated_at = ?
    WHERE textage_id = ?
    """,
        (
            version,
            title,
            title_search_key,
            artist,
            genre,
            is_ac_active,
            is_inf_active,
            now,
            now,
            textage_id,
        ),
    )
    return music_id


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def upsert_chart(
    conn: sqlite3.Connection,
    music_id: int,
    play_style: str,
    difficulty: str,
    level: int,
    notes: int,
    is_active: int,
    is_ac_active: int,
    is_inf_active: int,
) -> None:
    """chart 1件を Upsert する。"""
    cur = conn.cursor()
    now = now_iso()

    cur.execute(
        """
    SELECT chart_id FROM chart
    WHERE music_id = ? AND play_style = ? AND difficulty = ?
    """,
        (music_id, play_style, difficulty),
    )
    row = cur.fetchone()

    if row is None:
        cur.execute(
            """
        INSERT INTO chart (
            music_id, play_style, difficulty,
            level, notes, is_active, is_ac_active, is_inf_active,
            last_seen_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                music_id,
                play_style,
                difficulty,
                level,
                notes,
                is_active,
                is_ac_active,
                is_inf_active,
                now,
                now,
                now,
            ),
        )
        return

    cur.execute(
        """
    UPDATE chart SET
        level = ?,
        notes = ?,
        is_active = ?,
        is_ac_active = ?,
        is_inf_active = ?,
        last_seen_at = ?,
        updated_at = ?
    WHERE music_id = ? AND play_style = ? AND difficulty = ?
    """,
        (
            level,
            notes,
            is_active,
            is_ac_active,
            is_inf_active,
            now,
            now,
            music_id,
            play_style,
            difficulty,
        ),
    )


# pylint: disable-next=too-many-arguments,too-many-positional-arguments,too-many-locals
def build_or_update_sqlite(
    sqlite_path: str,
    titletbl: dict,
    datatbl: dict,
    actbl: dict,
    reset_flags: bool = True,
    schema_version: str = "1",
    asset_updated_at: str | None = None,
    manual_alias_csv_path: str | None = DEFAULT_MANUAL_ALIAS_CSV_PATH,
    manual_alias_csv_paths: list[str] | tuple[str, ...] | None = None,
    inf_music_index_url: str | None = None,
    inf_pack_csv_path: str = DEFAULT_INF_PACK_CSV_PATH,
) -> dict:
    """
    Textage テーブルから SQLite DB を構築または更新する。
    """
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")

    ensure_schema(conn)

    if reset_flags:
        reset_all_music_active_flags(conn)

    music_processed = 0
    chart_processed = 0
    ignored = 0
    explicit_title_qualifier_by_textage_id: dict[str, str] = {}

    for tag, row in titletbl.items():
        if tag not in datatbl or tag not in actbl:
            ignored += 1
            continue

        version_raw = str(row[0])
        version = "SS" if version_raw == "-35" else version_raw
        # textage_id must be stable and unique across updates; titletbl key satisfies this.
        textage_id = str(tag)

        genre = normalize_textage_string(row[3])
        artist = normalize_textage_string(row[4])
        title = normalize_textage_string(row[5])

        if len(row) > 6 and row[6]:
            subtitle = normalize_textage_string(row[6])
            if subtitle:
                title = f"{title} {subtitle}"

        act_row = actbl[tag]
        flags = _parse_textage_hex_or_int(act_row[0])
        is_ac_active = 1 if (flags & SONG_FLAG_AC) else 0
        is_inf_active = 1 if (flags & SONG_FLAG_INF) else 0

        music_id = upsert_music(
            conn,
            textage_id=textage_id,
            version=version,
            title=title,
            artist=artist,
            genre=genre,
            is_ac_active=is_ac_active,
            is_inf_active=is_inf_active,
        )
        explicit_qualifier = _extract_actbl_title_qualifier(act_row)
        if explicit_qualifier:
            explicit_title_qualifier_by_textage_id[textage_id] = explicit_qualifier
        music_processed += 1

        for chart_type, play_style, difficulty, act_index in CHART_TYPES:
            notes = datatbl[tag][chart_type]
            lv_int = _parse_textage_hex_or_int(act_row[act_index])
            chart_opt = _parse_textage_hex_or_int(act_row[act_index + 1])
            is_active = 1 if lv_int > 0 else 0
            chart_is_ac_active, chart_is_inf_active = _resolve_chart_scope_activity(
                song_flags=flags,
                chart_type=chart_type,
                level=lv_int,
                chart_opt=chart_opt,
            )

            upsert_chart(
                conn=conn,
                music_id=music_id,
                play_style=play_style,
                difficulty=difficulty,
                level=lv_int,
                notes=int(notes),
                is_active=is_active,
                is_ac_active=chart_is_ac_active,
                is_inf_active=chart_is_inf_active,
            )
            chart_processed += 1

    resolve_music_title_qualifiers(
        conn=conn,
        explicit_title_qualifier_by_textage_id=explicit_title_qualifier_by_textage_id,
    )

    alias_report = rebuild_music_title_aliases(
        conn=conn,
        manual_alias_csv_path=manual_alias_csv_path,
        manual_alias_csv_paths=manual_alias_csv_paths,
    )

    inf_pack_seed_report: dict | None = None
    inf_unlock_report: dict | None = None
    if inf_music_index_url:
        inf_unlock_report = apply_inf_unlock_information(
            conn=conn,
            inf_music_index_url=inf_music_index_url,
            inf_pack_csv_path=inf_pack_csv_path,
        )
        inf_pack_seed_report = inf_unlock_report["inf_pack_seed"]
        print(
            "[inf-unlock] parsed_entry_count="
            f"{inf_unlock_report['parsed_entry_count']} "
            "updated_music_rows="
            f"{inf_unlock_report['updated_music_rows']} "
            "unmatched_title_count="
            f"{inf_unlock_report['unmatched_title_count']} "
            "unresolved_pack_name_count="
            f"{inf_unlock_report['unresolved_pack_name_count']}"
        )
    else:
        inf_pack_seed_report = seed_inf_pack_table(
            conn=conn,
            inf_pack_csv_path=inf_pack_csv_path,
        )
        print(
            "[inf-pack] seeded from csv "
            f"rows={inf_pack_seed_report['db_row_count']}"
        )

    asset_value = asset_updated_at or now_iso()
    upsert_meta(
        conn,
        schema_version=schema_version,
        asset_updated_at=asset_value,
        generated_at=now_iso(),
    )

    conn.commit()
    conn.close()

    result = {
        "music_processed": music_processed,
        "chart_processed": chart_processed,
        "ignored": ignored,
        "official_alias_count": alias_report["official_alias_count"],
        "inserted_manual_alias_count": alias_report["inserted_manual_alias_count"],
        "skipped_redundant_manual_alias_count": alias_report[
            "skipped_redundant_manual_alias_count"
        ],
    }
    if inf_pack_seed_report is not None:
        result["inf_pack_seed"] = inf_pack_seed_report
    if inf_unlock_report is not None:
        result["inf_unlock"] = inf_unlock_report

    return result
