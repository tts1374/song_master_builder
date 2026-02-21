"""Validation helpers for music_title_alias integrity."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class AliasVerificationSummary:
    """Post-build alias validation summary."""

    active_music_count: int
    official_alias_count: int
    unresolved_official_music_count: int
    orphan_alias_count: int


def verify_music_title_alias_integrity(conn: sqlite3.Connection) -> AliasVerificationSummary:
    """Run required alias integrity checks and raise on failure."""
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM music
        WHERE is_ac_active = 1 OR is_inf_active = 1;
        """
    )
    active_music_count = int(cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM music_title_alias WHERE alias_type='official';")
    official_alias_count = int(cur.fetchone()[0])

    if official_alias_count != active_music_count:
        raise RuntimeError(
            "official alias count mismatch: "
            f"active_music={active_music_count}, official_alias={official_alias_count}"
        )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM music m
        LEFT JOIN music_title_alias a
          ON a.textage_id = m.textage_id
         AND a.alias_type = 'official'
        WHERE (m.is_ac_active = 1 OR m.is_inf_active = 1)
          AND a.alias_id IS NULL;
        """
    )
    unresolved_official_music_count = int(cur.fetchone()[0])
    if unresolved_official_music_count > 0:
        raise RuntimeError(
            "some songs do not have official aliases: "
            f"{unresolved_official_music_count}"
        )

    cur.execute(
        """
        SELECT alias, COUNT(*) AS c
        FROM music_title_alias
        GROUP BY alias
        HAVING c > 1
        LIMIT 1;
        """
    )
    duplicate = cur.fetchone()
    if duplicate is not None:
        raise RuntimeError(f"duplicate alias detected: {duplicate[0]}")

    cur.execute(
        """
        SELECT COUNT(*)
        FROM music_title_alias a
        LEFT JOIN music m ON m.textage_id = a.textage_id
        WHERE m.textage_id IS NULL;
        """
    )
    orphan_alias_count = int(cur.fetchone()[0])
    if orphan_alias_count > 0:
        raise RuntimeError(f"orphan aliases detected: {orphan_alias_count}")

    return AliasVerificationSummary(
        active_music_count=active_music_count,
        official_alias_count=official_alias_count,
        unresolved_official_music_count=unresolved_official_music_count,
        orphan_alias_count=orphan_alias_count,
    )
