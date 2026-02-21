"""Tests for official/wiki alias seeding into SQLite."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.generator.alias_seed_official import reset_music_title_aliases, seed_official_aliases
from src.generator.alias_seed_wiki import seed_wiki_aliases
from src.sqlite_builder import ensure_schema, upsert_music
from src.wiki.bemaniwiki_parse_title_alias import WikiAliasRow, parse_bemaniwiki_title_alias_table

FIXTURE_PATH = Path("tests/fixtures/bemaniwiki_title_alias.html")


def _insert_music(conn: sqlite3.Connection, textage_id: str, title: str):
    upsert_music(
        conn=conn,
        textage_id=textage_id,
        version="33",
        title=title,
        artist="ARTIST",
        genre="GENRE",
        is_ac_active=1,
        is_inf_active=1,
    )


@pytest.mark.light
def test_seed_wiki_aliases_from_fixture():
    """Fixture conversion rows resolve by title and insert csv_wiki aliases."""
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_music(conn, "T001", "\u707c\u71b1Beach Side Bunny")
        _insert_music(conn, "T002", "L'amour et la libert\u00e9")
        _insert_music(conn, "T003", "V\u00d8ID")
        conn.commit()

        wiki_rows, _ = parse_bemaniwiki_title_alias_table(
            FIXTURE_PATH.read_text(encoding="utf-8")
        )

        reset_music_title_aliases(conn)
        seed_official_aliases(conn, "2026-01-01T00:00:00Z")
        report = seed_wiki_aliases(
            conn=conn,
            wiki_rows=wiki_rows,
            now_utc_iso="2026-01-01T00:00:00Z",
        )

        assert report.inserted_csv_wiki_alias_count == 4
        assert report.dedup_skipped_count == 0
        assert report.max_csv_wiki_candidates_per_song == 2
        assert len(report.unresolved_official_titles) == 0

        csv_wiki_aliases = [
            row[0]
            for row in conn.execute(
                """
                SELECT alias
                FROM music_title_alias
                WHERE alias_type='csv_wiki'
                ORDER BY alias;
                """
            ).fetchall()
        ]
        assert csv_wiki_aliases == [
            "L'amour et la liberte",
            "Lamour et la liberte",
            "VOID",
            "\u707c\u71b1B",
        ]
    finally:
        conn.close()


@pytest.mark.light
def test_seed_wiki_aliases_skips_replaced_equal_official():
    """replaced_title equal to official_title should not be inserted."""
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_music(conn, "A001", "Song A")
        conn.commit()

        reset_music_title_aliases(conn)
        seed_official_aliases(conn, "2026-01-01T00:00:00Z")

        report = seed_wiki_aliases(
            conn=conn,
            wiki_rows=[
                WikiAliasRow(
                    official_title="Song A",
                    replaced_titles=("Song A", "Song Alias"),
                    note="",
                )
            ],
            now_utc_iso="2026-01-01T00:00:00Z",
        )

        assert report.inserted_csv_wiki_alias_count == 1
        assert report.dedup_skipped_count == 1
        inserted = conn.execute(
            """
            SELECT alias
            FROM music_title_alias
            WHERE alias_type='csv_wiki'
            ORDER BY alias;
            """
        ).fetchall()
        assert inserted == [("Song Alias",)]
    finally:
        conn.close()


@pytest.mark.light
def test_seed_wiki_aliases_fails_on_global_alias_collision():
    """Same alias mapped from different songs must fail as ambiguous."""
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_music(conn, "A001", "Song A")
        _insert_music(conn, "B001", "Song B")
        conn.commit()

        reset_music_title_aliases(conn)
        seed_official_aliases(conn, "2026-01-01T00:00:00Z")

        with pytest.raises(RuntimeError):
            seed_wiki_aliases(
                conn=conn,
                wiki_rows=[
                    WikiAliasRow(
                        official_title="Song A",
                        replaced_titles=("Shared Alias",),
                        note="",
                    ),
                    WikiAliasRow(
                        official_title="Song B",
                        replaced_titles=("Shared Alias",),
                        note="",
                    ),
                ],
                now_utc_iso="2026-01-01T00:00:00Z",
            )
    finally:
        conn.close()
