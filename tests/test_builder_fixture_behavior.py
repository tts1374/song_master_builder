"""Fixture behavior tests for sqlite_builder."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from src.sqlite_builder import (
    CHART_TYPES,
    build_or_update_sqlite,
    ensure_schema,
    resolve_music_title_qualifiers,
    upsert_music,
)


def _make_title_row(
    *,
    version: str = "33",
    legacy_textage_id: str = "T001",
    genre: str = "GENRE",
    artist: str = "ARTIST",
    title: str = "TITLE",
) -> list:
    return [version, legacy_textage_id, "", genre, artist, title]


def _make_data_row(*, base_notes: int = 100) -> list[int]:
    row = [0] * 11
    for chart_type, _, _, _ in CHART_TYPES:
        row[chart_type] = base_notes + chart_type
    return row


def _make_act_row(
    *,
    flags_hex: str = "03",
    default_level_hex: str = "5",
    level_overrides: dict[int, str] | None = None,
    title_qualifier: str | None = None,
) -> list:
    row = [0] * 24
    row[0] = flags_hex
    for chart_type, _, _, _ in CHART_TYPES:
        row[chart_type * 2 + 1] = default_level_hex
    if level_overrides:
        for chart_type, lv_hex in level_overrides.items():
            row[chart_type * 2 + 1] = lv_hex
    if title_qualifier is not None:
        row[23] = title_qualifier
    return row


def _read_music_row(conn: sqlite3.Connection, textage_id: str) -> tuple:
    return conn.execute(
        """
        SELECT music_id, created_at, updated_at, last_seen_at
        FROM music
        WHERE textage_id = ?
        """,
        (textage_id,),
    ).fetchone()


@pytest.mark.light
def test_fixture_parsing_and_missing_rows_are_ignored(tmp_path: Path):
    sqlite_path = tmp_path / "fixture.sqlite"

    titletbl = {
        "ok": _make_title_row(title="OK"),
        "missing_data": _make_title_row(title="NO_DATA"),
        "missing_act": _make_title_row(title="NO_ACT"),
    }
    datatbl = {
        "ok": _make_data_row(),
        "missing_act": _make_data_row(),
    }
    actbl = {
        "ok": _make_act_row(),
        "missing_data": _make_act_row(),
    }

    result = build_or_update_sqlite(
        sqlite_path=str(sqlite_path),
        titletbl=titletbl,
        datatbl=datatbl,
        actbl=actbl,
        schema_version="33",
        manual_alias_csv_path=None,
    )
    assert result["music_processed"] == 1
    assert result["ignored"] == 2

    conn = sqlite3.connect(str(sqlite_path))
    try:
        assert conn.execute("SELECT COUNT(*) FROM music;").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM chart;").fetchone()[0] == len(CHART_TYPES)
        assert conn.execute("SELECT COUNT(*) FROM music_title_alias;").fetchone()[0] == 2
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM music_title_alias WHERE alias_type='official';"
            ).fetchone()[0]
            == 2
        )
    finally:
        conn.close()


@pytest.mark.light
def test_invalid_hex_level_in_actbl_fails(tmp_path: Path):
    sqlite_path = tmp_path / "invalid_hex.sqlite"
    titletbl = {"bad": _make_title_row(title="BAD")}
    datatbl = {"bad": _make_data_row()}
    actbl = {"bad": _make_act_row(level_overrides={2: "ZZ"})}

    with pytest.raises(ValueError):
        build_or_update_sqlite(
            sqlite_path=str(sqlite_path),
            titletbl=titletbl,
            datatbl=datatbl,
            actbl=actbl,
            schema_version="33",
            manual_alias_csv_path=None,
        )


@pytest.mark.light
def test_lightweight_schema_minimum_constraints(tmp_path: Path):
    sqlite_path = tmp_path / "schema_minimum.sqlite"
    build_or_update_sqlite(
        sqlite_path=str(sqlite_path),
        titletbl={"song": _make_title_row()},
        datatbl={"song": _make_data_row()},
        actbl={"song": _make_act_row()},
        schema_version="33",
        manual_alias_csv_path=None,
    )

    conn = sqlite3.connect(str(sqlite_path))
    try:
        cols = {row[1]: row for row in conn.execute("PRAGMA table_info(music);").fetchall()}
        assert "textage_id" in cols
        assert "title_qualifier" in cols
        assert "title_search_key" in cols
        assert cols["textage_id"][3] == 1
        assert cols["title_qualifier"][3] == 1
        assert cols["title_search_key"][3] == 1

        idx = conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='index' AND name='idx_music_title_search_key';
            """
        ).fetchone()
        assert idx is not None

        alias_cols = {
            row[1]: row for row in conn.execute("PRAGMA table_info(music_title_alias);").fetchall()
        }
        assert "textage_id" in alias_cols
        assert "alias_scope" in alias_cols
        assert "alias" in alias_cols
        assert "alias_type" in alias_cols
        assert alias_cols["textage_id"][3] == 1
        assert alias_cols["alias_scope"][3] == 1
        assert alias_cols["alias"][3] == 1
        assert alias_cols["alias_type"][3] == 1

        assert conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='index' AND name='uq_music_title_alias_scope_alias';
            """
        ).fetchone() is not None
        assert conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='index' AND name='idx_music_title_alias_textage_id';
            """
        ).fetchone() is not None
        assert conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='index' AND name='idx_music_title_alias_scope_alias';
            """
        ).fetchone() is not None
        assert conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='index' AND name='uq_music_title_alias_textage_scope_alias';
            """
        ).fetchone() is not None
    finally:
        conn.close()


@pytest.mark.light
def test_diff_update_converges_and_updates_flags(tmp_path: Path):
    sqlite_path = tmp_path / "update.sqlite"
    textage_id = "song"

    build_or_update_sqlite(
        sqlite_path=str(sqlite_path),
        titletbl={"song": _make_title_row(legacy_textage_id="C001", title="Song V1")},
        datatbl={"song": _make_data_row(base_notes=200)},
        actbl={"song": _make_act_row(default_level_hex="5")},
        schema_version="33",
        manual_alias_csv_path=None,
    )

    conn = sqlite3.connect(str(sqlite_path))
    try:
        before = _read_music_row(conn, textage_id)
        assert before is not None
    finally:
        conn.close()

    time.sleep(0.01)

    build_or_update_sqlite(
        sqlite_path=str(sqlite_path),
        titletbl={"song": _make_title_row(legacy_textage_id="C999", title="Song V2")},
        datatbl={"song": _make_data_row(base_notes=250)},
        actbl={"song": _make_act_row(default_level_hex="5", level_overrides={2: "0"})},
        schema_version="33",
        manual_alias_csv_path=None,
    )

    conn = sqlite3.connect(str(sqlite_path))
    try:
        after = _read_music_row(conn, textage_id)
        assert after is not None
        assert after[0] == before[0]
        assert after[1] == before[1]
        assert after[2] != before[2]
        assert after[3] != before[3]

        assert conn.execute(
            "SELECT COUNT(*) FROM music WHERE textage_id = ?;",
            (textage_id,),
        ).fetchone()[0] == 1

        duplicated = conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT music_id, play_style, difficulty, COUNT(*) AS c
              FROM chart
              GROUP BY music_id, play_style, difficulty
              HAVING c > 1
            );
            """
        ).fetchone()[0]
        assert duplicated == 0

        is_active = conn.execute(
            """
            SELECT c.is_active
            FROM chart c
            INNER JOIN music m ON m.music_id = c.music_id
            WHERE m.textage_id = ?
              AND c.play_style = 'SP'
              AND c.difficulty = 'NORMAL'
            """,
            (textage_id,),
        ).fetchone()[0]
        assert is_active == 0
    finally:
        conn.close()


@pytest.mark.light
def test_title_qualifier_resolution_priority_and_fallback():
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        upsert_music(
            conn=conn,
            textage_id="Q001",
            version="33",
            title="DUP",
            artist="ARTIST",
            genre="GENRE",
            is_ac_active=1,
            is_inf_active=0,
        )
        upsert_music(
            conn=conn,
            textage_id="Q002",
            version="33",
            title="DUP",
            artist="ARTIST",
            genre="GENRE",
            is_ac_active=0,
            is_inf_active=1,
        )
        upsert_music(
            conn=conn,
            textage_id="Q003",
            version="33",
            title="DUP",
            artist="ARTIST",
            genre="GENRE",
            is_ac_active=1,
            is_inf_active=1,
        )
        upsert_music(
            conn=conn,
            textage_id="Q004",
            version="33",
            title="EXPLICIT",
            artist="ARTIST",
            genre="GENRE",
            is_ac_active=1,
            is_inf_active=0,
        )
        upsert_music(
            conn=conn,
            textage_id="Q005",
            version="33",
            title="EXPLICIT",
            artist="ARTIST",
            genre="GENRE",
            is_ac_active=0,
            is_inf_active=1,
        )
        upsert_music(
            conn=conn,
            textage_id="Q006",
            version="33",
            title="SINGLE",
            artist="ARTIST",
            genre="GENRE",
            is_ac_active=1,
            is_inf_active=0,
        )

        resolve_music_title_qualifiers(
            conn=conn,
            explicit_title_qualifier_by_textage_id={"Q004": "(CS9th)"},
        )

        resolved = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT textage_id, title_qualifier FROM music ORDER BY textage_id;"
            ).fetchall()
        }
        assert resolved["Q001"] == "(AC)"
        assert resolved["Q002"] == "(INF)"
        assert resolved["Q003"] == ""
        assert resolved["Q004"] == "(CS9th)"
        assert resolved["Q005"] == "(INF)"
        assert resolved["Q006"] == ""
    finally:
        conn.close()


@pytest.mark.light
def test_build_sets_title_qualifier_from_actbl_note(tmp_path: Path):
    sqlite_path = tmp_path / "qualifier_from_actbl.sqlite"
    titletbl = {
        "dup_ac": _make_title_row(legacy_textage_id="A101", title="DUP"),
        "dup_inf": _make_title_row(legacy_textage_id="A102", title="DUP"),
        "explicit": _make_title_row(legacy_textage_id="A103", title="EXPLICIT"),
        "single": _make_title_row(legacy_textage_id="A104", title="SINGLE"),
    }
    datatbl = {key: _make_data_row() for key in titletbl}
    actbl = {
        "dup_ac": _make_act_row(flags_hex="01"),
        "dup_inf": _make_act_row(flags_hex="02"),
        "explicit": _make_act_row(
            flags_hex="01", title_qualifier="<span style='font-size:9pt'>(CS9th)</span>"
        ),
        "single": _make_act_row(flags_hex="01"),
    }

    build_or_update_sqlite(
        sqlite_path=str(sqlite_path),
        titletbl=titletbl,
        datatbl=datatbl,
        actbl=actbl,
        schema_version="33",
        manual_alias_csv_path=None,
    )

    conn = sqlite3.connect(str(sqlite_path))
    try:
        resolved = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT textage_id, title_qualifier FROM music ORDER BY textage_id;"
            ).fetchall()
        }
        assert resolved["dup_ac"] == "(AC)"
        assert resolved["dup_inf"] == "(INF)"
        assert resolved["explicit"] == "(CS9th)"
        assert resolved["single"] == ""
    finally:
        conn.close()


@pytest.mark.light
def test_build_uses_titletbl_key_as_textage_id(tmp_path: Path):
    sqlite_path = tmp_path / "textage_id_from_tag.sqlite"
    titletbl = {
        "acidvis": _make_title_row(legacy_textage_id="3905", title="ACID VISION"),
        "a_galaxy": _make_title_row(legacy_textage_id="3905", title="Around The Galaxy"),
    }
    datatbl = {key: _make_data_row() for key in titletbl}
    actbl = {key: _make_act_row() for key in titletbl}

    build_or_update_sqlite(
        sqlite_path=str(sqlite_path),
        titletbl=titletbl,
        datatbl=datatbl,
        actbl=actbl,
        schema_version="33",
        manual_alias_csv_path=None,
    )

    conn = sqlite3.connect(str(sqlite_path))
    try:
        rows = conn.execute(
            "SELECT textage_id, title FROM music ORDER BY textage_id;"
        ).fetchall()
        assert rows == [
            ("a_galaxy", "Around The Galaxy"),
            ("acidvis", "ACID VISION"),
        ]
    finally:
        conn.close()
