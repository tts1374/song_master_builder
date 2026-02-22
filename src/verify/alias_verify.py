"""Validation helpers for music_title_alias integrity."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class AliasVerificationSummary:
    """Post-build alias validation summary."""

    active_ac_music_count: int
    active_inf_music_count: int
    official_ac_alias_count: int
    official_inf_alias_count: int
    unresolved_official_scope_count: int
    orphan_alias_count: int


def verify_music_title_alias_integrity(conn: sqlite3.Connection) -> AliasVerificationSummary:
    """Run required alias integrity checks and raise on failure."""
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM music WHERE is_ac_active = 1;")
    active_ac_music_count = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM music WHERE is_inf_active = 1;")
    active_inf_music_count = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT COUNT(*)
        FROM music_title_alias
        WHERE alias_type='official' AND alias_scope='ac';
        """
    )
    official_ac_alias_count = int(cur.fetchone()[0])
    cur.execute(
        """
        SELECT COUNT(*)
        FROM music_title_alias
        WHERE alias_type='official' AND alias_scope='inf';
        """
    )
    official_inf_alias_count = int(cur.fetchone()[0])

    if official_ac_alias_count != active_ac_music_count:
        raise RuntimeError(
            "official alias count mismatch for ac: "
            f"active_ac_music={active_ac_music_count}, official_ac_alias={official_ac_alias_count}"
        )
    if official_inf_alias_count != active_inf_music_count:
        raise RuntimeError(
            "official alias count mismatch for inf: "
            f"active_inf_music={active_inf_music_count}, "
            f"official_inf_alias={official_inf_alias_count}"
        )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM music m
        LEFT JOIN music_title_alias a
          ON a.textage_id = m.textage_id
         AND a.alias_type = 'official'
         AND a.alias_scope = 'ac'
        WHERE m.is_ac_active = 1
          AND a.alias_id IS NULL;
        """
    )
    unresolved_official_ac_count = int(cur.fetchone()[0])
    cur.execute(
        """
        SELECT COUNT(*)
        FROM music m
        LEFT JOIN music_title_alias a
          ON a.textage_id = m.textage_id
         AND a.alias_type = 'official'
         AND a.alias_scope = 'inf'
        WHERE m.is_inf_active = 1
          AND a.alias_id IS NULL;
        """
    )
    unresolved_official_inf_count = int(cur.fetchone()[0])
    unresolved_official_scope_count = unresolved_official_ac_count + unresolved_official_inf_count
    if unresolved_official_scope_count > 0:
        raise RuntimeError(
            "some active scope rows do not have official aliases: "
            f"ac_missing={unresolved_official_ac_count}, inf_missing={unresolved_official_inf_count}"
        )

    cur.execute(
        """
        SELECT alias_scope, alias, COUNT(*) AS c
        FROM music_title_alias
        GROUP BY alias_scope, alias
        HAVING c > 1
        LIMIT 1;
        """
    )
    duplicate = cur.fetchone()
    if duplicate is not None:
        raise RuntimeError(f"duplicate alias detected: {duplicate[0]}:{duplicate[1]}")

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

    cur.execute(
        """
        SELECT alias_type, COUNT(*) AS c
        FROM music_title_alias
        WHERE alias_type NOT IN ('official', 'manual')
        GROUP BY alias_type
        ORDER BY alias_type;
        """
    )
    invalid_alias_types = cur.fetchall()
    if invalid_alias_types:
        sample = ", ".join(
            f"{row[0]}:{int(row[1])}" for row in invalid_alias_types[:10]
        )
        raise RuntimeError(f"invalid alias_type values detected: {sample}")

    return AliasVerificationSummary(
        active_ac_music_count=active_ac_music_count,
        active_inf_music_count=active_inf_music_count,
        official_ac_alias_count=official_ac_alias_count,
        official_inf_alias_count=official_inf_alias_count,
        unresolved_official_scope_count=unresolved_official_scope_count,
        orphan_alias_count=orphan_alias_count,
    )
