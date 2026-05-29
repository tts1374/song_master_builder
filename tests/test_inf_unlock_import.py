"""Tests for INF unlock type / pack import workflow."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from urllib.error import HTTPError

import pytest

from src.sqlite_builder import (
    apply_inf_unlock_information,
    ensure_schema,
    fetch_inf_music_index_html,
    parse_inf_unlock_entries_from_music_index_html,
    reset_all_music_active_flags,
    upsert_music,
)


def _insert_inf_alias(
    conn: sqlite3.Connection,
    *,
    textage_id: str,
    alias: str,
    alias_type: str = "manual",
) -> None:
    now = "2026-03-17T00:00:00Z"
    conn.execute(
        """
        INSERT INTO music_title_alias (
            textage_id,
            alias_scope,
            alias,
            alias_type,
            created_at,
            updated_at
        )
        VALUES (?, 'inf', ?, ?, ?, ?)
        """,
        (textage_id, alias, alias_type, now, now),
    )


def _seed_music_row(
    conn: sqlite3.Connection,
    *,
    textage_id: str,
    title: str,
    is_inf_active: int,
) -> None:
    upsert_music(
        conn,
        textage_id=textage_id,
        version="test",
        title=title,
        artist="artist",
        genre="genre",
        is_ac_active=1,
        is_inf_active=is_inf_active,
    )


@pytest.mark.light
def test_parse_inf_unlock_entries_from_music_index_html_extracts_required_categories():
    html = """
    <div class="cat" id="default"><strong>初期収録曲</strong></div>
    <table><tr><th>タイトル</th></tr><tr><td>Song Initial</td><td>A</td></tr></table>
    <div class="cat" id="djp"><strong>DJP解禁曲</strong></div>
    <table><tr><th>タイトル</th></tr><tr><td>Song DJP</td><td>A</td></tr></table>
    <div class="cat" id="bit"><strong>BIT解禁曲</strong></div>
    <table><tr><th>タイトル</th></tr><tr><td>Song BIT</td><td>A</td></tr></table>
    <div class="cat" id="pac"><strong>楽曲パック</strong></div>
    <div class="cat" id="pac_vol1"><strong>beatmania IIDX INFINITAS 楽曲パック vol.1<br>( TEST PACK )</strong></div>
    <table><tr><th>タイトル</th></tr><tr><td>Song PACK</td><td>A</td></tr></table>
    """

    entries = parse_inf_unlock_entries_from_music_index_html(html)
    assert ("Song Initial", "initial", None) in {
        (entry.title, entry.unlock_type, entry.pack_name) for entry in entries
    }
    assert ("Song DJP", "djp", None) in {
        (entry.title, entry.unlock_type, entry.pack_name) for entry in entries
    }
    assert ("Song BIT", "bit", None) in {
        (entry.title, entry.unlock_type, entry.pack_name) for entry in entries
    }
    assert ("Song PACK", "pack", "楽曲パック vol.1( TEST PACK )") in {
        (entry.title, entry.unlock_type, entry.pack_name) for entry in entries
    }


@pytest.mark.light
def test_fetch_inf_music_index_html_retries_transient_http_error(
    monkeypatch: pytest.MonkeyPatch,
):
    attempts = {"count": 0}

    class _Headers:
        def get_content_charset(self):
            return "utf-8"

    class _Response:
        headers = _Headers()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"ok"

    def fake_urlopen(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise HTTPError(
                url="https://example.invalid/inf",
                code=503,
                msg="Service Unavailable",
                hdrs=None,
                fp=None,
            )
        return _Response()

    monkeypatch.setattr("src.sqlite_builder.urllib_request.urlopen", fake_urlopen)

    html = fetch_inf_music_index_html(
        "https://example.invalid/inf",
        max_attempts=2,
        retry_sleep_sec=0,
    )

    assert html == "ok"
    assert attempts["count"] == 2


@pytest.mark.light
def test_apply_inf_unlock_information_updates_music_with_alias_exact_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    sqlite_path = tmp_path / "song_master.sqlite"
    inf_pack_csv_path = tmp_path / "inf_pack.csv"
    inf_pack_csv_path.write_text(
        "pack_code,pack_name,display_order\n"
        "pack_test_01,楽曲パック vol.1( TEST PACK ),1\n",
        encoding="utf-8",
    )

    conn = sqlite3.connect(str(sqlite_path))
    try:
        ensure_schema(conn)
        _seed_music_row(conn, textage_id="T001", title="Song Initial", is_inf_active=1)
        _seed_music_row(conn, textage_id="T002", title="Song Pack", is_inf_active=1)
        _seed_music_row(conn, textage_id="T003", title="Song Inactive", is_inf_active=0)
        _insert_inf_alias(conn, textage_id="T001", alias="Alias Initial")
        _insert_inf_alias(conn, textage_id="T002", alias="Alias Pack")
        _insert_inf_alias(conn, textage_id="T003", alias="Alias Inactive")
        conn.commit()

        html = """
        <div class="cat" id="default"><strong>初期収録曲</strong></div>
        <table>
          <tr><th>タイトル</th><th>アーティスト名</th></tr>
          <tr><td>Alias Initial</td><td>A</td></tr>
          <tr><td>Unknown Alias</td><td>A</td></tr>
        </table>
        <div class="cat" id="djp"><strong>DJP解禁曲</strong></div>
        <table><tr><th>タイトル</th><th>アーティスト名</th></tr></table>
        <div class="cat" id="bit"><strong>BIT解禁曲</strong></div>
        <table><tr><th>タイトル</th><th>アーティスト名</th></tr><tr><td>Alias Inactive</td><td>A</td></tr></table>
        <div class="cat" id="pac"><strong>楽曲パック</strong></div>
        <div class="cat" id="pac_vol1"><strong>beatmania IIDX INFINITAS 楽曲パック vol.1<br>( TEST PACK )</strong></div>
        <table><tr><th>タイトル</th><th>アーティスト名</th></tr><tr><td>Alias Pack</td><td>A</td></tr></table>
        """

        monkeypatch.setattr(
            "src.sqlite_builder.fetch_inf_music_index_html",
            lambda *_args, **_kwargs: html,
        )

        report = apply_inf_unlock_information(
            conn=conn,
            inf_music_index_url="https://example.invalid/inf",
            inf_pack_csv_path=str(inf_pack_csv_path),
            inf_unlock_override_csv_path="",
        )
        conn.commit()

        assert report["updated_music_rows"] == 2
        assert report["skipped_non_inf_active_rows"] == 1
        assert report["unmatched_title_count"] == 1
        assert report["unresolved_pack_name_count"] == 0

        rows = conn.execute(
            """
            SELECT textage_id, is_inf_active, inf_unlock_type, inf_pack_id
            FROM music
            ORDER BY textage_id
            """
        ).fetchall()
        assert rows[0][0] == "T001"
        assert rows[0][2] == "initial"
        assert rows[0][3] is None

        assert rows[1][0] == "T002"
        assert rows[1][2] == "pack"
        assert rows[1][3] is not None

        assert rows[2][0] == "T003"
        assert rows[2][1] == 0
        assert rows[2][2] is None
        assert rows[2][3] is None
    finally:
        conn.close()


@pytest.mark.light
def test_apply_inf_unlock_information_preserves_existing_unlocks_when_fetch_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    sqlite_path = tmp_path / "song_master.sqlite"
    conn = sqlite3.connect(str(sqlite_path))
    try:
        ensure_schema(conn)
        _seed_music_row(conn, textage_id="T001", title="Song Initial", is_inf_active=1)
        _seed_music_row(conn, textage_id="T002", title="Song Removed", is_inf_active=1)
        conn.execute(
            """
            UPDATE music
            SET inf_unlock_type = 'initial',
                inf_pack_id = NULL
            WHERE textage_id IN ('T001', 'T002')
            """
        )
        conn.commit()

        reset_all_music_active_flags(conn)
        _seed_music_row(conn, textage_id="T001", title="Song Initial", is_inf_active=1)
        conn.commit()

        def raise_fetch_error(*_args, **_kwargs):
            raise RuntimeError("failed to fetch INFINITAS music page after 3 attempts")

        monkeypatch.setattr(
            "src.sqlite_builder.fetch_inf_music_index_html",
            raise_fetch_error,
        )

        report = apply_inf_unlock_information(
            conn=conn,
            inf_music_index_url="https://example.invalid/inf",
            inf_pack_csv_path="",
            inf_unlock_override_csv_path="",
        )
        conn.commit()

        assert report["skipped"] is True
        assert report["updated_music_rows"] == 0
        assert report["cleared_inactive_rows"] == 1
        rows = conn.execute(
            """
            SELECT textage_id, is_inf_active, inf_unlock_type, inf_pack_id
            FROM music
            ORDER BY textage_id
            """
        ).fetchall()
        assert rows == [
            ("T001", 1, "initial", None),
            ("T002", 0, None, None),
        ]
    finally:
        conn.close()


@pytest.mark.light
def test_apply_inf_unlock_information_skips_pack_when_pack_name_not_in_csv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    sqlite_path = tmp_path / "song_master.sqlite"
    inf_pack_csv_path = tmp_path / "inf_pack.csv"
    inf_pack_csv_path.write_text(
        "pack_code,pack_name,display_order\n"
        "pack_test_01,楽曲パック vol.1( CSV PACK ),1\n",
        encoding="utf-8",
    )

    conn = sqlite3.connect(str(sqlite_path))
    try:
        ensure_schema(conn)
        _seed_music_row(conn, textage_id="T002", title="Song Pack", is_inf_active=1)
        _insert_inf_alias(conn, textage_id="T002", alias="Alias Pack")
        conn.commit()

        html = """
        <div class="cat" id="default"><strong>初期収録曲</strong></div>
        <table><tr><th>タイトル</th><th>アーティスト名</th></tr></table>
        <div class="cat" id="djp"><strong>DJP解禁曲</strong></div>
        <table><tr><th>タイトル</th><th>アーティスト名</th></tr></table>
        <div class="cat" id="bit"><strong>BIT解禁曲</strong></div>
        <table><tr><th>タイトル</th><th>アーティスト名</th></tr></table>
        <div class="cat" id="pac"><strong>楽曲パック</strong></div>
        <div class="cat" id="pac_vol1"><strong>beatmania IIDX INFINITAS 楽曲パック vol.1<br>( PAGE PACK )</strong></div>
        <table><tr><th>タイトル</th><th>アーティスト名</th></tr><tr><td>Alias Pack</td><td>A</td></tr></table>
        """
        monkeypatch.setattr(
            "src.sqlite_builder.fetch_inf_music_index_html",
            lambda *_args, **_kwargs: html,
        )

        report = apply_inf_unlock_information(
            conn=conn,
            inf_music_index_url="https://example.invalid/inf",
            inf_pack_csv_path=str(inf_pack_csv_path),
            inf_unlock_override_csv_path="",
        )
        conn.commit()

        assert report["updated_music_rows"] == 0
        assert report["unresolved_pack_name_count"] == 1

        row = conn.execute(
            "SELECT inf_unlock_type, inf_pack_id FROM music WHERE textage_id = 'T002'"
        ).fetchone()
        assert row == (None, None)
    finally:
        conn.close()


@pytest.mark.light
def test_parse_inf_unlock_entries_from_music_index_html_parses_sale_pack_and_newsong_bit():
    html = """
    <div class="cat" id="default"><strong>初期収録曲</strong></div>
    <table><tr><th>タイトル</th></tr><tr><td>Song Initial</td><td>A</td></tr></table>
    <div class="cat" id="djp"><strong>DJP解禁曲</strong></div>
    <table><tr><th>タイトル</th></tr><tr><td>Song DJP</td><td>A</td></tr></table>
    <div class="cat" id="bit"><strong>BIT解禁曲</strong></div>
    <table><tr><th>タイトル</th></tr><tr><td>Song BIT</td><td>A</td></tr></table>
    <div class="cat" id="newsong"><strong>新規追加曲</strong></div>
    <div class="cat"><strong>2026/3/4追加</strong> BIT解禁曲</div>
    <table>
      <tr><th>タイトル</th></tr>
      <tr><td>#CMFLG</td><td>A</td></tr>
      <tr><td>Banger Banger Banger Banger</td><td>A</td></tr>
    </table>
    <div class="cat" id="sale2"><strong>beatmania IIDX INFINITAS 楽曲パック vol.7<br>( 21 SPADA )</strong></div>
    <table><tr><th>タイトル</th></tr><tr><td>Song Sale Pack</td><td>A</td></tr></table>
    """

    entries = parse_inf_unlock_entries_from_music_index_html(html)
    rows = {(entry.title, entry.unlock_type, entry.pack_name) for entry in entries}

    assert ("#CMFLG", "bit", None) in rows
    assert ("Banger Banger Banger Banger", "bit", None) in rows
    assert ("Song Sale Pack", "pack", "楽曲パック vol.7( 21 SPADA )") in rows
