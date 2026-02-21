"""配布物（SQLite/latest.json）の必須整合性テスト。"""

from __future__ import annotations

import datetime as dt
import hashlib
import sqlite3
from pathlib import Path

import pytest

from src.build_validation import validate_chart_id_stability


def _sha256_hex(path: Path) -> str:
    """ファイルの SHA-256（16進）を返す。"""
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_sql(sql: str) -> str:
    """SQL定義を空白差分に頑健な比較用文字列へ正規化する。"""
    return " ".join((sql or "").lower().split())


@pytest.mark.required
@pytest.mark.full
def test_generated_sqlite_integrity_and_constraints(artifact_paths: dict):
    """PRAGMA と sqlite_master で生成SQLiteの必須要件を検証する。"""
    sqlite_path: Path = artifact_paths["sqlite_path"]
    assert sqlite_path.exists(), f"SQLite が存在しません: {sqlite_path}"

    conn = sqlite3.connect(str(sqlite_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchall() == [("ok",)]
        assert conn.execute("PRAGMA quick_check;").fetchall() == [("ok",)]
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        music_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='music';"
        ).fetchone()
        assert music_sql is not None
        music_sql_norm = _normalize_sql(music_sql[0])
        assert "textage_id text not null unique" in music_sql_norm

        chart_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='chart';"
        ).fetchone()
        assert chart_sql is not None
        chart_sql_norm = _normalize_sql(chart_sql[0])
        assert "unique(music_id, play_style, difficulty)" in chart_sql_norm

        alias_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='music_title_alias';"
        ).fetchone()
        assert alias_sql is not None
        alias_sql_norm = _normalize_sql(alias_sql[0])
        assert "alias text not null" in alias_sql_norm
        assert "alias_type text not null" in alias_sql_norm

        music_cols = {row[1]: row for row in conn.execute("PRAGMA table_info(music);").fetchall()}
        assert music_cols["textage_id"][3] == 1
        assert music_cols["title_search_key"][3] == 1

        idx = conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='index' AND name='idx_music_title_search_key';
            """
        ).fetchone()
        assert idx is not None
        assert conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='index' AND name='uq_music_title_alias_alias';
            """
        ).fetchone() is not None
        assert conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='index' AND name='idx_music_title_alias_textage_id';
            """
        ).fetchone() is not None

        music_count = conn.execute("SELECT COUNT(*) FROM music;").fetchone()[0]
        official_alias_count = conn.execute(
            "SELECT COUNT(*) FROM music_title_alias WHERE alias_type='official';"
        ).fetchone()[0]
        assert music_count == official_alias_count
    finally:
        conn.close()


@pytest.mark.required
@pytest.mark.full
def test_latest_json_integrity(artifact_paths: dict):
    """latest.json の必須キーとハッシュ/サイズ整合を検証する。"""
    latest_json_path: Path = artifact_paths["latest_json_path"]
    sqlite_path: Path = artifact_paths["sqlite_path"]
    manifest: dict = artifact_paths["manifest"]

    required_keys = {"file_name", "schema_version", "generated_at", "sha256", "byte_size"}
    missing = required_keys - set(manifest.keys())
    assert not missing, f"latest.json の必須キー不足: {missing}"

    assert manifest["file_name"] == sqlite_path.name
    assert sqlite_path.exists()
    assert int(manifest["byte_size"]) == sqlite_path.stat().st_size
    assert manifest["sha256"] == _sha256_hex(sqlite_path)
    dt.datetime.fromisoformat(str(manifest["generated_at"]).replace("Z", "+00:00"))

    conn = sqlite3.connect(str(sqlite_path))
    try:
        row = conn.execute(
            """
            SELECT schema_version
            FROM meta
            ORDER BY rowid DESC
            LIMIT 1;
            """
        ).fetchone()
        assert row is not None, "meta.schema_version が存在しません"
        assert str(row[0]) == str(manifest["schema_version"])
    finally:
        conn.close()

    assert latest_json_path.exists()


@pytest.mark.required
@pytest.mark.full
def test_chart_id_stability_against_baseline(baseline_sqlite_path: Path, artifact_paths: dict):
    """baseline との比較で chart_id 永続性を検証する。"""
    sqlite_path: Path = artifact_paths["sqlite_path"]
    summary = validate_chart_id_stability(
        old_sqlite_path=str(baseline_sqlite_path),
        new_sqlite_path=str(sqlite_path),
        missing_policy="warn",
    )
    assert summary["old_total"] == 0 or summary["shared_total"] > 0
