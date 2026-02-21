"""Seed official aliases into music_title_alias."""

from __future__ import annotations

import sqlite3

ALIAS_TYPE_OFFICIAL = "official"


def reset_music_title_aliases(conn: sqlite3.Connection):
    """Delete all existing aliases before full rebuild."""
    conn.execute("DELETE FROM music_title_alias;")


def seed_official_aliases(conn: sqlite3.Connection, now_utc_iso: str) -> int:
    """Insert exactly one official alias (music.title) for each music row."""
    cur = conn.cursor()
    cur.execute("SELECT textage_id, title FROM music ORDER BY music_id;")
    rows = cur.fetchall()

    params = [
        (
            textage_id,
            title,
            ALIAS_TYPE_OFFICIAL,
            now_utc_iso,
            now_utc_iso,
        )
        for textage_id, title in rows
    ]

    cur.executemany(
        """
        INSERT INTO music_title_alias (
            textage_id, alias, alias_type, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        params,
    )
    return len(params)
