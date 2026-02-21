"""Seed official aliases into music_title_alias."""

from __future__ import annotations

import sqlite3

ALIAS_TYPE_OFFICIAL = "official"


def reset_music_title_aliases(conn: sqlite3.Connection):
    """Delete all existing aliases before full rebuild."""
    conn.execute("DELETE FROM music_title_alias;")


def seed_official_aliases(conn: sqlite3.Connection, now_utc_iso: str) -> int:
    """Insert official alias only for active songs."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT textage_id, title
        FROM music
        WHERE is_ac_active = 1 OR is_inf_active = 1
        ORDER BY music_id;
        """
    )
    rows = cur.fetchall()

    first_seen_by_alias: dict[str, str] = {}
    duplicate_aliases: dict[str, list[str]] = {}
    for textage_id, title in rows:
        key = str(title)
        tid = str(textage_id)
        if key not in first_seen_by_alias:
            first_seen_by_alias[key] = tid
            continue
        duplicate_aliases.setdefault(key, [first_seen_by_alias[key]]).append(tid)

    if duplicate_aliases:
        sample = "; ".join(
            f"{alias!r}: {','.join(ids)}"
            for alias, ids in list(duplicate_aliases.items())[:10]
        )
        raise RuntimeError(
            "official alias collision detected in music.title "
            f"(duplicate_titles={len(duplicate_aliases)}): {sample}"
        )

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
