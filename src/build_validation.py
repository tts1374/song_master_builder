"""Validation helpers for SQLite artifacts and latest.json manifest."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO8601 format with Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def file_sha256(path: str) -> str:
    """Compute SHA-256 hex digest for a file."""
    digest = hashlib.sha256()
    with open(path, "rb") as file_obj:
        while True:
            chunk = file_obj.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def file_byte_size(path: str) -> int:
    """Return file size in bytes."""
    return os.path.getsize(path)


def build_latest_manifest(
    sqlite_path: str,
    schema_version: str,
    generated_at: str,
    source_hashes: dict[str, str] | None = None,
) -> dict:
    """Build manifest payload for latest.json."""
    manifest = {
        "file_name": os.path.basename(sqlite_path),
        "schema_version": schema_version,
        "generated_at": generated_at,
        "sha256": file_sha256(sqlite_path),
        "byte_size": file_byte_size(sqlite_path),
    }
    if source_hashes:
        manifest["source_hashes"] = source_hashes
    return manifest


def write_latest_manifest(latest_json_path: str, manifest: dict):
    """Write latest.json in UTF-8 with trailing newline."""
    os.makedirs(os.path.dirname(latest_json_path) or ".", exist_ok=True)
    with open(latest_json_path, "w", encoding="utf-8") as file_obj:
        json.dump(manifest, file_obj, ensure_ascii=False, indent=2)
        file_obj.write("\n")


def validate_latest_manifest(latest_json_path: str, sqlite_path: str):
    """Validate latest.json metadata against actual SQLite file."""
    with open(latest_json_path, "r", encoding="utf-8") as file_obj:
        manifest = json.load(file_obj)

    actual_sha = file_sha256(sqlite_path)
    actual_size = file_byte_size(sqlite_path)
    actual_name = os.path.basename(sqlite_path)

    if manifest.get("file_name") != actual_name:
        raise RuntimeError(
            "latest.json file_name mismatch: "
            f"{manifest.get('file_name')} != {actual_name}"
        )
    if manifest.get("sha256") != actual_sha:
        raise RuntimeError("latest.json sha256 mismatch")
    if manifest.get("byte_size") != actual_size:
        raise RuntimeError("latest.json byte_size mismatch")


def _index_columns(conn: sqlite3.Connection, index_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA index_info({index_name});")
    rows = cur.fetchall()
    return [row[2] for row in rows]


def _has_unique_index(
    conn: sqlite3.Connection,
    table_name: str,
    expected_columns: list[str],
) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA index_list({table_name});")
    for row in cur.fetchall():
        index_name = row[1]
        is_unique = row[2]
        if is_unique != 1:
            continue
        if _index_columns(conn, index_name) == expected_columns:
            return True
    return False


def _assert_not_null_column(conn: sqlite3.Connection, table_name: str, column_name: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name});")
    for row in cur.fetchall():
        if row[1] == column_name:
            if row[3] != 1:
                raise RuntimeError(f"{table_name}.{column_name} must be NOT NULL")
            return
    raise RuntimeError(f"column not found: {table_name}.{column_name}")


def _assert_index_exists(conn: sqlite3.Connection, table_name: str, index_name: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA index_list({table_name});")
    names = {row[1] for row in cur.fetchall()}
    if index_name not in names:
        raise RuntimeError(f"index not found: {index_name}")


def _read_meta_schema_version(conn: sqlite3.Connection) -> str:
    """Read meta.schema_version from SQLite."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT schema_version
        FROM meta
        ORDER BY rowid DESC
        LIMIT 1;
        """
    )
    row = cur.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("meta.schema_version not found")
    return str(row[0])


def validate_db_schema_and_data(sqlite_path: str, expected_schema_version: str | None = None):
    """Validate required schema and minimal data constraints for generated SQLite."""
    conn = sqlite3.connect(sqlite_path)
    try:
        _assert_not_null_column(conn, "music", "textage_id")
        _assert_not_null_column(conn, "music", "title_qualifier")
        _assert_not_null_column(conn, "music", "title_search_key")
        _assert_not_null_column(conn, "music_title_alias", "textage_id")
        _assert_not_null_column(conn, "music_title_alias", "alias_scope")
        _assert_not_null_column(conn, "music_title_alias", "alias")
        _assert_not_null_column(conn, "music_title_alias", "alias_type")

        if not _has_unique_index(conn, "music", ["textage_id"]):
            raise RuntimeError("music.textage_id unique index is missing")

        if not _has_unique_index(conn, "chart", ["music_id", "play_style", "difficulty"]):
            raise RuntimeError("chart unique index is missing")

        if not _has_unique_index(conn, "music_title_alias", ["alias_scope", "alias"]):
            raise RuntimeError("music_title_alias(alias_scope, alias) unique index is missing")

        _assert_index_exists(conn, "music", "idx_music_title_search_key")
        _assert_index_exists(conn, "music_title_alias", "idx_music_title_alias_textage_id")
        _assert_index_exists(conn, "music_title_alias", "uq_music_title_alias_scope_alias")
        _assert_index_exists(conn, "music_title_alias", "idx_music_title_alias_scope_alias")
        _assert_index_exists(conn, "music_title_alias", "uq_music_title_alias_textage_scope_alias")

        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM music WHERE title_search_key IS NULL;")
        null_count = int(cur.fetchone()[0])
        if null_count > 0:
            raise RuntimeError(f"title_search_key has {null_count} NULL rows")

        cur.execute("SELECT COUNT(*) FROM music WHERE title_qualifier IS NULL;")
        null_title_qualifier_count = int(cur.fetchone()[0])
        if null_title_qualifier_count > 0:
            raise RuntimeError(f"title_qualifier has {null_title_qualifier_count} NULL rows")

        cur.execute(
            """
            SELECT COUNT(*)
            FROM music
            WHERE title_qualifier <> ''
              AND (INSTR(title_qualifier, '(') > 0 OR INSTR(title_qualifier, ')') > 0)
              AND (SUBSTR(title_qualifier, 1, 1) <> '(' OR SUBSTR(title_qualifier, -1, 1) <> ')');
            """
        )
        malformed_title_qualifier_count = int(cur.fetchone()[0])
        if malformed_title_qualifier_count > 0:
            raise RuntimeError(
                "title_qualifier format mismatch detected: "
                f"{malformed_title_qualifier_count}"
            )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM music m
            INNER JOIN (
                SELECT title
                FROM music
                GROUP BY title
                HAVING COUNT(textage_id) > 1
            ) dup ON dup.title = m.title
            WHERE m.title_qualifier = ''
              AND (
                   (m.is_ac_active = 1 AND m.is_inf_active = 0)
                OR (m.is_ac_active = 0 AND m.is_inf_active = 1)
              );
            """
        )
        missing_collision_title_qualifier_count = int(cur.fetchone()[0])
        if missing_collision_title_qualifier_count > 0:
            raise RuntimeError(
                "title collision rows with single-scope activity must have title_qualifier: "
                f"{missing_collision_title_qualifier_count}"
            )

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

        if active_ac_music_count != official_ac_alias_count:
            raise RuntimeError(
                "official alias count mismatch for ac: "
                f"active_ac_music={active_ac_music_count}, "
                f"official_ac_alias={official_ac_alias_count}"
            )
        if active_inf_music_count != official_inf_alias_count:
            raise RuntimeError(
                "official alias count mismatch for inf: "
                f"active_inf_music={active_inf_music_count}, "
                f"official_inf_alias={official_inf_alias_count}"
            )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM music_title_alias a
            LEFT JOIN music m ON m.textage_id = a.textage_id
            WHERE m.textage_id IS NULL;
            """
        )
        orphan_count = int(cur.fetchone()[0])
        if orphan_count > 0:
            raise RuntimeError(f"music_title_alias has {orphan_count} orphan rows")

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
            raise RuntimeError(
                "music_title_alias has invalid alias_type values: "
                f"{sample}"
            )

        actual_schema_version = _read_meta_schema_version(conn)
        if expected_schema_version is not None and actual_schema_version != str(
            expected_schema_version
        ):
            raise RuntimeError(
                "meta.schema_version mismatch: "
                f"{actual_schema_version} != {expected_schema_version}"
            )
    finally:
        conn.close()


def _load_chart_key_map(sqlite_path: str) -> dict[tuple[str, str, str], int]:
    """Load chart_id by stable business key (textage_id, play_style, difficulty)."""
    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT m.textage_id, c.play_style, c.difficulty, c.chart_id
            FROM chart c
            INNER JOIN music m ON m.music_id = c.music_id
            """
        )
        rows = cur.fetchall()
        return {(row[0], row[1], row[2]): int(row[3]) for row in rows}
    finally:
        conn.close()


def validate_chart_id_stability(
    old_sqlite_path: str,
    new_sqlite_path: str,
    missing_policy: str = "error",
) -> dict:
    """Validate that chart_id remains stable for shared business keys."""
    if missing_policy not in {"error", "warn"}:
        raise ValueError("missing_policy must be 'error' or 'warn'")

    old_map = _load_chart_key_map(old_sqlite_path)
    new_map = _load_chart_key_map(new_sqlite_path)

    mismatches: list[tuple[tuple[str, str, str], int, int]] = []
    missing_in_new: list[tuple[str, str, str]] = []

    for key, old_chart_id in old_map.items():
        new_chart_id = new_map.get(key)
        if new_chart_id is None:
            missing_in_new.append(key)
            continue
        if new_chart_id != old_chart_id:
            mismatches.append((key, old_chart_id, new_chart_id))

    if mismatches:
        sample = ", ".join(
            [
                f"{k[0]}/{k[1]}/{k[2]} old={old_id} new={new_id}"
                for k, old_id, new_id in mismatches[:10]
            ]
        )
        raise RuntimeError(
            f"chart_id mismatches detected ({len(mismatches)}): {sample}"
        )

    if missing_in_new and missing_policy == "error":
        sample = ", ".join([f"{k[0]}/{k[1]}/{k[2]}" for k in missing_in_new[:10]])
        raise RuntimeError(
            "new sqlite is missing charts from old sqlite "
            f"({len(missing_in_new)}): {sample}"
        )

    return {
        "old_total": len(old_map),
        "new_total": len(new_map),
        "shared_total": len(old_map) - len(missing_in_new),
        "new_only_total": len(new_map) - (len(old_map) - len(missing_in_new)),
        "missing_in_new_total": len(missing_in_new),
        "missing_policy": missing_policy,
    }
