"""Tests for official/manual alias seeding into SQLite."""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from src.generator.alias_seed_manual import seed_manual_aliases_from_csv
from src.generator.alias_seed_official import reset_music_title_aliases, seed_official_aliases
from src.sqlite_builder import ensure_schema, upsert_music


def _insert_music(
    conn: sqlite3.Connection,
    textage_id: str,
    title: str,
    is_ac_active: int = 1,
    is_inf_active: int = 1,
):
    upsert_music(
        conn=conn,
        textage_id=textage_id,
        version="33",
        title=title,
        artist="ARTIST",
        genre="GENRE",
        is_ac_active=is_ac_active,
        is_inf_active=is_inf_active,
    )


def _write_manual_alias_csv(path: Path, rows: list[dict]) -> Path:
    with path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "textage_id",
                "title_canon",  # ignored by importer
                "alias",
                "alias_scope",
                "alias_type",
                "note",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


@pytest.mark.light
def test_seed_official_aliases_respects_active_scope_flags():
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_music(conn, "A001", "Song A", is_ac_active=1, is_inf_active=0)
        _insert_music(conn, "B001", "Song B", is_ac_active=0, is_inf_active=1)
        _insert_music(conn, "C001", "Song C", is_ac_active=1, is_inf_active=1)
        _insert_music(conn, "D001", "Song D", is_ac_active=0, is_inf_active=0)
        conn.commit()

        reset_music_title_aliases(conn)
        inserted = seed_official_aliases(conn, "2026-01-01T00:00:00Z")

        assert inserted == 4
        aliases = conn.execute(
            """
            SELECT alias_scope, textage_id, alias_type, alias
            FROM music_title_alias
            ORDER BY alias_scope, textage_id;
            """
        ).fetchall()
        assert aliases == [
            ("ac", "A001", "official", "Song A"),
            ("ac", "C001", "official", "Song C"),
            ("inf", "B001", "official", "Song B"),
            ("inf", "C001", "official", "Song C"),
        ]
    finally:
        conn.close()


@pytest.mark.light
def test_seed_manual_aliases_from_csv_inserts_rows(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_music(conn, "A001", "Song A", is_ac_active=1, is_inf_active=0)
        _insert_music(conn, "B001", "Song B", is_ac_active=0, is_inf_active=1)
        conn.commit()

        csv_path = _write_manual_alias_csv(
            tmp_path / "music_alias_manual.csv",
            [
                {
                    "textage_id": "A001",
                    "title_canon": "Song A",
                    "alias": "Song Alias A",
                    "alias_scope": "ac",
                    "alias_type": "manual",
                    "note": "",
                },
                {
                    "textage_id": "B001",
                    "title_canon": "Song B",
                    "alias": "Song Alias B",
                    "alias_scope": "inf",
                    "alias_type": "manual",
                    "note": "manual note",
                },
            ],
        )

        reset_music_title_aliases(conn)
        seed_official_aliases(conn, "2026-01-01T00:00:00Z")
        report = seed_manual_aliases_from_csv(
            conn=conn,
            csv_path=csv_path,
            now_utc_iso="2026-01-01T00:00:00Z",
        )

        assert report.inserted_manual_alias_count == 2
        assert report.skipped_redundant_manual_alias_count == 0

        manual_rows = conn.execute(
            """
            SELECT alias_scope, textage_id, alias_type, alias
            FROM music_title_alias
            WHERE alias_type='manual'
            ORDER BY alias_scope, textage_id;
            """
        ).fetchall()
        assert manual_rows == [
            ("ac", "A001", "manual", "Song Alias A"),
            ("inf", "B001", "manual", "Song Alias B"),
        ]
    finally:
        conn.close()


@pytest.mark.light
def test_seed_manual_aliases_fails_on_orphan_textage_id(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_music(conn, "A001", "Song A", is_ac_active=1, is_inf_active=0)
        conn.commit()

        csv_path = _write_manual_alias_csv(
            tmp_path / "music_alias_manual.csv",
            [
                {
                    "textage_id": "UNKNOWN",
                    "title_canon": "",
                    "alias": "Alias",
                    "alias_scope": "ac",
                    "alias_type": "manual",
                    "note": "",
                }
            ],
        )

        reset_music_title_aliases(conn)
        seed_official_aliases(conn, "2026-01-01T00:00:00Z")

        with pytest.raises(RuntimeError, match="textage_id not found"):
            seed_manual_aliases_from_csv(
                conn=conn,
                csv_path=csv_path,
                now_utc_iso="2026-01-01T00:00:00Z",
            )
    finally:
        conn.close()


@pytest.mark.light
def test_seed_manual_aliases_fails_on_csv_duplicate_scope_alias(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_music(conn, "A001", "Song A", is_ac_active=1, is_inf_active=0)
        _insert_music(conn, "B001", "Song B", is_ac_active=1, is_inf_active=0)
        conn.commit()

        csv_path = _write_manual_alias_csv(
            tmp_path / "music_alias_manual.csv",
            [
                {
                    "textage_id": "A001",
                    "title_canon": "",
                    "alias": "Shared Alias",
                    "alias_scope": "ac",
                    "alias_type": "manual",
                    "note": "",
                },
                {
                    "textage_id": "B001",
                    "title_canon": "",
                    "alias": "Shared Alias",
                    "alias_scope": "ac",
                    "alias_type": "manual",
                    "note": "",
                },
            ],
        )

        reset_music_title_aliases(conn)
        seed_official_aliases(conn, "2026-01-01T00:00:00Z")

        with pytest.raises(RuntimeError, match="duplicate \\(alias_scope, alias\\)"):
            seed_manual_aliases_from_csv(
                conn=conn,
                csv_path=csv_path,
                now_utc_iso="2026-01-01T00:00:00Z",
            )
    finally:
        conn.close()


@pytest.mark.light
def test_seed_manual_aliases_fails_on_official_collision(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_music(conn, "A001", "Song A", is_ac_active=1, is_inf_active=0)
        _insert_music(conn, "B001", "Song B", is_ac_active=1, is_inf_active=0)
        conn.commit()

        csv_path = _write_manual_alias_csv(
            tmp_path / "music_alias_manual.csv",
            [
                {
                    "textage_id": "B001",
                    "title_canon": "",
                    "alias": "Song A",  # collides with official alias in AC scope
                    "alias_scope": "ac",
                    "alias_type": "manual",
                    "note": "",
                }
            ],
        )

        reset_music_title_aliases(conn)
        seed_official_aliases(conn, "2026-01-01T00:00:00Z")

        with pytest.raises(RuntimeError, match="UNIQUE\\(alias_scope, alias\\)"):
            seed_manual_aliases_from_csv(
                conn=conn,
                csv_path=csv_path,
                now_utc_iso="2026-01-01T00:00:00Z",
            )
    finally:
        conn.close()


@pytest.mark.light
def test_seed_manual_aliases_skips_redundant_same_as_official(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_music(conn, "A001", "Song A", is_ac_active=1, is_inf_active=0)
        conn.commit()

        csv_path = _write_manual_alias_csv(
            tmp_path / "music_alias_manual.csv",
            [
                {
                    "textage_id": "A001",
                    "title_canon": "",
                    "alias": "Song A",
                    "alias_scope": "ac",
                    "alias_type": "manual",
                    "note": "",
                }
            ],
        )

        reset_music_title_aliases(conn)
        seed_official_aliases(conn, "2026-01-01T00:00:00Z")
        report = seed_manual_aliases_from_csv(
            conn=conn,
            csv_path=csv_path,
            now_utc_iso="2026-01-01T00:00:00Z",
        )

        assert report.inserted_manual_alias_count == 0
        assert report.skipped_redundant_manual_alias_count == 1

        manual_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM music_title_alias
            WHERE alias_type='manual';
            """
        ).fetchone()[0]
        assert manual_count == 0
    finally:
        conn.close()

