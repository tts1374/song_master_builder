"""Seed official aliases into music_title_alias."""

from __future__ import annotations

import sqlite3

ALIAS_TYPE_OFFICIAL = "official"
ALIAS_SCOPE_AC = "ac"
ALIAS_SCOPE_INF = "inf"


def reset_music_title_aliases(conn: sqlite3.Connection):
    """Delete all existing aliases before full rebuild."""
    conn.execute("DELETE FROM music_title_alias;")


def seed_official_aliases(conn: sqlite3.Connection, now_utc_iso: str) -> int:
    """Insert official aliases for active scopes (ac / inf)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT textage_id, title, is_ac_active, is_inf_active
        FROM music
        WHERE is_ac_active = 1 OR is_inf_active = 1
        ORDER BY music_id;
        """
    )
    rows = cur.fetchall()

    params: list[tuple[str, str, str, str, str, str]] = []
    first_seen_by_scope_alias: dict[tuple[str, str], str] = {}
    duplicate_scope_aliases: dict[tuple[str, str], list[str]] = {}

    for textage_id, title, is_ac_active, is_inf_active in rows:
        title_value = str(title)
        tid = str(textage_id)

        if int(is_ac_active) == 1:
            params.append(
                (
                    ALIAS_SCOPE_AC,
                    tid,
                    title_value,
                    ALIAS_TYPE_OFFICIAL,
                    now_utc_iso,
                    now_utc_iso,
                )
            )
            key_ac = (ALIAS_SCOPE_AC, title_value)
            if key_ac not in first_seen_by_scope_alias:
                first_seen_by_scope_alias[key_ac] = tid
            else:
                duplicate_scope_aliases.setdefault(key_ac, [first_seen_by_scope_alias[key_ac]]).append(
                    tid
                )

        if int(is_inf_active) == 1:
            params.append(
                (
                    ALIAS_SCOPE_INF,
                    tid,
                    title_value,
                    ALIAS_TYPE_OFFICIAL,
                    now_utc_iso,
                    now_utc_iso,
                )
            )
            key_inf = (ALIAS_SCOPE_INF, title_value)
            if key_inf not in first_seen_by_scope_alias:
                first_seen_by_scope_alias[key_inf] = tid
            else:
                duplicate_scope_aliases.setdefault(
                    key_inf, [first_seen_by_scope_alias[key_inf]]
                ).append(tid)

    if duplicate_scope_aliases:
        sample = "; ".join(
            f"{scope}:{alias!r}: {','.join(ids)}"
            for (scope, alias), ids in list(duplicate_scope_aliases.items())[:10]
        )
        raise RuntimeError(
            "official alias collision detected in music.title by scope "
            f"(duplicate_scope_aliases={len(duplicate_scope_aliases)}): {sample}"
        )

    cur.executemany(
        """
        INSERT INTO music_title_alias (
            alias_scope, textage_id, alias, alias_type, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        params,
    )
    return len(params)
