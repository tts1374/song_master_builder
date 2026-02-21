"""
SQLite 楽曲マスターデータベースを構築・更新するモジュール。
"""

from __future__ import annotations

import html
import os
import re
import sqlite3
import unicodedata
from collections import Counter
from datetime import datetime, timedelta, timezone

import requests

from src.config import BemaniWikiAliasConfig
from src.generator.alias_seed_official import reset_music_title_aliases, seed_official_aliases
from src.generator.alias_seed_wiki import seed_wiki_aliases
from src.verify.alias_verify import verify_music_title_alias_integrity
from src.wiki.bemaniwiki_fetch import load_bemaniwiki_title_alias_html
from src.wiki.bemaniwiki_parse_title_alias import parse_bemaniwiki_title_alias_table

TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")

JST = timezone(timedelta(hours=9), "JST")

# 検索互換性に影響するため、置換定義はこの1箇所で管理する。
TITLE_SEARCH_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("ä", "a"),
    ("ö", "o"),
    ("ü", "u"),
    ("ß", "ss"),
    ("æ", "ae"),
    ("œ", "oe"),
    ("ø", "o"),
    ("å", "a"),
    ("ç", "c"),
    ("ñ", "n"),
    ("á", "a"),
    ("à", "a"),
    ("â", "a"),
    ("ã", "a"),
    ("é", "e"),
    ("è", "e"),
    ("ê", "e"),
    ("ë", "e"),
    ("í", "i"),
    ("ì", "i"),
    ("î", "i"),
    ("ï", "i"),
    ("ó", "o"),
    ("ò", "o"),
    ("ô", "o"),
    ("õ", "o"),
    ("ú", "u"),
    ("ù", "u"),
    ("û", "u"),
    ("ý", "y"),
    ("ÿ", "y"),
)


def normalize_textage_string(s: str) -> str:
    """Textage由来文字列を表示用に正規化する。"""
    if s is None:
        return ""

    value = str(s)
    value = html.unescape(value)
    value = TAG_RE.sub("", value)
    value = SPACE_RE.sub(" ", value).strip()
    return value


def normalize_title_search_key(title: str) -> str:
    """
    検索用タイトルキーを正規化する。

    仕様固定順序:
    1) 小文字化
    2) trim
    3) 置換テーブル
    4) NFD 分解 + 結合文字除去
    5) 連続空白圧縮
    """
    if title is None:
        value = ""
    else:
        value = str(title)

    value = value.lower()
    value = value.strip()

    for source, target in TITLE_SEARCH_REPLACEMENTS:
        value = value.replace(source, target)

    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = SPACE_RE.sub(" ", value)
    return value


def now_iso() -> str:
    """現在のJST時刻を ISO 8601 形式で返す。"""
    return datetime.now(JST).isoformat()


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO8601 with Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


CHART_TYPES = [
    # (type, play_style, difficulty, act_index)
    (1, "SP", "BEGINNER", 3),
    (2, "SP", "NORMAL", 5),
    (3, "SP", "HYPER", 7),
    (4, "SP", "ANOTHER", 9),
    (5, "SP", "LEGGENDARIA", 11),
    (7, "DP", "NORMAL", 15),
    (8, "DP", "HYPER", 17),
    (9, "DP", "ANOTHER", 19),
    (10, "DP", "LEGGENDARIA", 21),
]
ACTBL_TITLE_QUALIFIER_INDEX = 23


def download_latest_sqlite_from_release(
    owner: str,
    repo: str,
    sqlite_path: str,
    token: str | None = None,
    asset_name: str = "song_master.sqlite",
) -> dict:
    """
    最新リリースから指定名のアセットをダウンロードする。
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"

    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 404:
        return {"downloaded": False, "asset_updated_at": None}
    response.raise_for_status()
    release = response.json()

    target = None
    for asset in release.get("assets", []):
        if asset.get("name") == asset_name:
            target = asset
            break

    if not target or not target.get("browser_download_url"):
        return {"downloaded": False, "asset_updated_at": None}

    download_headers = {}
    if token:
        download_headers["Authorization"] = f"Bearer {token}"

    asset_response = requests.get(
        target["browser_download_url"], headers=download_headers, timeout=60
    )
    asset_response.raise_for_status()

    os.makedirs(os.path.dirname(sqlite_path) or ".", exist_ok=True)
    with open(sqlite_path, "wb") as file_obj:
        file_obj.write(asset_response.content)

    return {"downloaded": True, "asset_updated_at": target.get("updated_at")}


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name});")
    rows = cur.fetchall()
    return any(row[1] == column_name for row in rows)


def _backfill_title_search_keys(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT music_id, title, title_search_key FROM music;")

    updates: list[tuple[str, int]] = []
    for music_id, title, title_search_key in cur.fetchall():
        if title_search_key is None or title_search_key == "":
            updates.append((normalize_title_search_key(title), music_id))

    if updates:
        cur.executemany(
            "UPDATE music SET title_search_key = ? WHERE music_id = ?",
            updates,
        )


def _extract_actbl_title_qualifier(act_row: object) -> str:
    """Extract explicit display qualifier from actbl row when present."""
    if not isinstance(act_row, list):
        return ""
    if len(act_row) <= ACTBL_TITLE_QUALIFIER_INDEX:
        return ""

    raw_value = act_row[ACTBL_TITLE_QUALIFIER_INDEX]
    if not isinstance(raw_value, str):
        return ""

    qualifier = normalize_textage_string(raw_value)
    return qualifier


def resolve_music_title_qualifiers(
    conn: sqlite3.Connection,
    explicit_title_qualifier_by_textage_id: dict[str, str] | None = None,
):
    """
    Resolve display-only title qualifiers by these rules:
    1) explicit actbl qualifier wins
    2) if duplicate title and no explicit qualifier, fill (AC)/(INF) for single-scope actives
    3) otherwise empty
    """
    explicit_map = explicit_title_qualifier_by_textage_id or {}
    cur = conn.cursor()
    cur.execute(
        """
        SELECT music_id, textage_id, title, is_ac_active, is_inf_active, title_qualifier
        FROM music
        ORDER BY music_id;
        """
    )
    rows = cur.fetchall()
    title_counts = Counter(str(row[2]) for row in rows)

    updates: list[tuple[str, int]] = []
    for music_id, textage_id, title, is_ac_active, is_inf_active, current_qualifier in rows:
        textage_key = str(textage_id)
        explicit_qualifier = explicit_map.get(textage_key, "")
        if explicit_qualifier:
            resolved = explicit_qualifier
        elif title_counts[str(title)] > 1:
            ac = int(is_ac_active)
            inf = int(is_inf_active)
            if ac == 1 and inf == 0:
                resolved = "(AC)"
            elif ac == 0 and inf == 1:
                resolved = "(INF)"
            else:
                resolved = ""
        else:
            resolved = ""

        if str(current_qualifier or "") != resolved:
            updates.append((resolved, int(music_id)))

    if updates:
        cur.executemany(
            "UPDATE music SET title_qualifier = ? WHERE music_id = ?",
            updates,
        )


def ensure_schema(conn: sqlite3.Connection):
    """DBスキーマの作成・移行を行う。"""
    cur = conn.cursor()

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS music (
        music_id INTEGER PRIMARY KEY AUTOINCREMENT,
        textage_id TEXT NOT NULL UNIQUE,
        version TEXT NOT NULL,
        title TEXT NOT NULL,
        title_qualifier TEXT NOT NULL DEFAULT '',
        title_search_key TEXT NOT NULL,
        artist TEXT NOT NULL,
        genre TEXT NOT NULL,
        is_ac_active INTEGER NOT NULL,
        is_inf_active INTEGER NOT NULL,
        last_seen_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS chart (
        chart_id INTEGER PRIMARY KEY AUTOINCREMENT,
        music_id INTEGER NOT NULL,
        play_style TEXT NOT NULL,
        difficulty TEXT NOT NULL,
        level INTEGER NOT NULL,
        notes INTEGER NOT NULL,
        is_active INTEGER NOT NULL,
        last_seen_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(music_id, play_style, difficulty),
        FOREIGN KEY(music_id) REFERENCES music(music_id)
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS meta (
        schema_version TEXT NOT NULL,
        asset_updated_at TEXT NOT NULL,
        generated_at TEXT NOT NULL
    );
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS music_title_alias (
        alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
        textage_id TEXT NOT NULL,
        alias_scope TEXT NOT NULL CHECK(alias_scope IN ('ac', 'inf')),
        alias TEXT NOT NULL,
        alias_type TEXT NOT NULL CHECK(alias_type IN ('official', 'csv_wiki', 'manual')),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(textage_id) REFERENCES music(textage_id)
    );
    """
    )

    if _table_exists(conn, "music") and not _column_exists(
        conn, "music", "title_search_key"
    ):
        cur.execute(
            "ALTER TABLE music ADD COLUMN title_search_key TEXT NOT NULL DEFAULT ''"
        )

    if _table_exists(conn, "music") and not _column_exists(
        conn, "music", "title_qualifier"
    ):
        cur.execute(
            "ALTER TABLE music ADD COLUMN title_qualifier TEXT NOT NULL DEFAULT ''"
        )

    if _table_exists(conn, "music_title_alias") and not _column_exists(
        conn, "music_title_alias", "alias_scope"
    ):
        cur.execute(
            "ALTER TABLE music_title_alias ADD COLUMN alias_scope TEXT NOT NULL DEFAULT 'ac'"
        )

    _backfill_title_search_keys(conn)

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_chart_music_active ON chart(music_id, is_active);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_chart_filter "
        "ON chart(play_style, difficulty, level, is_active);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_chart_notes_active ON chart(is_active, notes);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_title_search_key "
        "ON music(title_search_key);"
    )
    cur.execute("DROP INDEX IF EXISTS uq_music_title_alias_alias;")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_title_alias_textage_id "
        "ON music_title_alias(textage_id);"
    )
    cur.execute("DROP INDEX IF EXISTS uq_music_title_alias_textage_alias;")
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_music_title_alias_scope_alias "
        "ON music_title_alias(alias_scope, alias);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_music_title_alias_scope_alias "
        "ON music_title_alias(alias_scope, alias);"
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_music_title_alias_textage_scope_alias "
        "ON music_title_alias(textage_id, alias_scope, alias);"
    )

    conn.commit()


def upsert_meta(
    conn: sqlite3.Connection,
    schema_version: str,
    asset_updated_at: str,
    generated_at: str,
):
    """meta テーブルを最新1行で更新する。"""
    cur = conn.cursor()
    cur.execute("DELETE FROM meta;")
    cur.execute(
        "INSERT INTO meta (schema_version, asset_updated_at, generated_at) VALUES (?, ?, ?)",
        (schema_version, asset_updated_at, generated_at),
    )
    conn.commit()


def reset_all_music_active_flags(conn: sqlite3.Connection):
    """取り込み前に収録フラグを全件リセットする。"""
    cur = conn.cursor()
    now = now_iso()

    cur.execute(
        """
    UPDATE music SET
        is_ac_active = 0,
        is_inf_active = 0,
        updated_at = ?
    """,
        (now,),
    )

    conn.commit()


def rebuild_music_title_aliases(
    conn: sqlite3.Connection,
    bemaniwiki_alias_config: BemaniWikiAliasConfig | None,
) -> dict:
    """Rebuild `music_title_alias` from official titles and optional wiki conversion table."""
    alias_timestamp = now_utc_iso()
    reset_music_title_aliases(conn)
    official_count = seed_official_aliases(conn, alias_timestamp)

    parse_report = None
    seed_report = None
    source = None
    encoding = None
    replacement_count = 0

    if bemaniwiki_alias_config is not None:
        document = load_bemaniwiki_title_alias_html(bemaniwiki_alias_config)
        source = document.source
        encoding = document.encoding
        replacement_count = document.replacement_char_count

        wiki_rows, parse_report = parse_bemaniwiki_title_alias_table(document.html_text)
        seed_report = seed_wiki_aliases(
            conn=conn,
            wiki_rows=wiki_rows,
            now_utc_iso=alias_timestamp,
            unresolved_official_title_fail_threshold=(
                bemaniwiki_alias_config.unresolved_official_title_fail_threshold
            ),
        )

        unresolved_count = len(seed_report.unresolved_official_titles)
        unresolved_sample = list(seed_report.unresolved_official_titles[:10])
        print(
            "[alias/wiki] table_selected="
            f"{parse_report.selected_table_index} scanned={parse_report.tables_scanned} "
            f"matched={parse_report.matched_tables}"
        )
        print(
            "[alias/wiki] rows total="
            f"{parse_report.parsed_rows_total} definitions={parse_report.definition_rows} "
            f"skipped={parse_report.skipped_rows_by_reason}"
        )
        print(
            "[alias/wiki] source="
            f"{source} encoding={encoding} replacements={replacement_count}"
        )
        print(
            "[alias/wiki] unresolved_official_titles_count="
            f"{unresolved_count} sample={unresolved_sample}"
        )
        print(
            "[alias/wiki] inserted_csv_wiki_alias_count="
            f"{seed_report.inserted_csv_wiki_alias_count} "
            f"dedup_skipped_count={seed_report.dedup_skipped_count} "
            "max_csv_wiki_candidates_per_song="
            f"{seed_report.max_csv_wiki_candidates_per_song}"
        )

    verify_summary = verify_music_title_alias_integrity(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT alias_scope, alias_type, COUNT(*) AS c
        FROM music_title_alias
        GROUP BY alias_scope, alias_type
        ORDER BY alias_scope, alias_type;
        """
    )
    scope_type_counts = {
        f"{row[0]}:{row[1]}": int(row[2])
        for row in cur.fetchall()
    }
    print(f"[alias] scope_type_counts={scope_type_counts}")

    return {
        "official_alias_count": official_count,
        "wiki_source": source,
        "wiki_encoding": encoding,
        "wiki_decode_replacement_count": replacement_count,
        "wiki_parsed_rows_total": (
            parse_report.parsed_rows_total if parse_report is not None else 0
        ),
        "wiki_definition_rows": (
            parse_report.definition_rows if parse_report is not None else 0
        ),
        "wiki_skipped_rows_by_reason": (
            parse_report.skipped_rows_by_reason if parse_report is not None else {}
        ),
        "unresolved_official_titles_count": (
            len(seed_report.unresolved_official_titles) if seed_report is not None else 0
        ),
        "unresolved_official_titles_sample": (
            list(seed_report.unresolved_official_titles[:10]) if seed_report is not None else []
        ),
        "inserted_csv_wiki_alias_count": (
            seed_report.inserted_csv_wiki_alias_count if seed_report is not None else 0
        ),
        "dedup_skipped_count": seed_report.dedup_skipped_count if seed_report is not None else 0,
        "max_csv_wiki_candidates_per_song": (
            seed_report.max_csv_wiki_candidates_per_song if seed_report is not None else 0
        ),
        "alias_ac_music_count": verify_summary.active_ac_music_count,
        "alias_inf_music_count": verify_summary.active_inf_music_count,
        "alias_official_ac_count": verify_summary.official_ac_alias_count,
        "alias_official_inf_count": verify_summary.official_inf_alias_count,
        "scope_type_counts": scope_type_counts,
    }


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def upsert_music(
    conn: sqlite3.Connection,
    textage_id: str,
    version: str,
    title: str,
    artist: str,
    genre: str,
    is_ac_active: int,
    is_inf_active: int,
) -> int:
    """music 1件を Upsert する。"""
    cur = conn.cursor()
    now = now_iso()
    title_search_key = normalize_title_search_key(title)

    cur.execute("SELECT music_id FROM music WHERE textage_id = ?", (textage_id,))
    row = cur.fetchone()

    if row is None:
        cur.execute(
            """
        INSERT INTO music (
            textage_id, version, title, title_search_key, artist, genre,
            is_ac_active, is_inf_active,
            last_seen_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                textage_id,
                version,
                title,
                title_search_key,
                artist,
                genre,
                is_ac_active,
                is_inf_active,
                now,
                now,
                now,
            ),
        )
        return cur.lastrowid

    music_id = row[0]
    cur.execute(
        """
    UPDATE music SET
        version = ?,
        title = ?,
        title_search_key = ?,
        artist = ?,
        genre = ?,
        is_ac_active = ?,
        is_inf_active = ?,
        last_seen_at = ?,
        updated_at = ?
    WHERE textage_id = ?
    """,
        (
            version,
            title,
            title_search_key,
            artist,
            genre,
            is_ac_active,
            is_inf_active,
            now,
            now,
            textage_id,
        ),
    )
    return music_id


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def upsert_chart(
    conn: sqlite3.Connection,
    music_id: int,
    play_style: str,
    difficulty: str,
    level: int,
    notes: int,
    is_active: int,
) -> None:
    """chart 1件を Upsert する。"""
    cur = conn.cursor()
    now = now_iso()

    cur.execute(
        """
    SELECT chart_id FROM chart
    WHERE music_id = ? AND play_style = ? AND difficulty = ?
    """,
        (music_id, play_style, difficulty),
    )
    row = cur.fetchone()

    if row is None:
        cur.execute(
            """
        INSERT INTO chart (
            music_id, play_style, difficulty,
            level, notes, is_active,
            last_seen_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                music_id,
                play_style,
                difficulty,
                level,
                notes,
                is_active,
                now,
                now,
                now,
            ),
        )
        return

    cur.execute(
        """
    UPDATE chart SET
        level = ?,
        notes = ?,
        is_active = ?,
        last_seen_at = ?,
        updated_at = ?
    WHERE music_id = ? AND play_style = ? AND difficulty = ?
    """,
        (
            level,
            notes,
            is_active,
            now,
            now,
            music_id,
            play_style,
            difficulty,
        ),
    )


# pylint: disable-next=too-many-arguments,too-many-positional-arguments,too-many-locals
def build_or_update_sqlite(
    sqlite_path: str,
    titletbl: dict,
    datatbl: dict,
    actbl: dict,
    reset_flags: bool = True,
    schema_version: str = "1",
    asset_updated_at: str | None = None,
    bemaniwiki_alias_config: BemaniWikiAliasConfig | None = None,
) -> dict:
    """
    Textage テーブルから SQLite DB を構築または更新する。
    """
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")

    ensure_schema(conn)

    if reset_flags:
        reset_all_music_active_flags(conn)

    music_processed = 0
    chart_processed = 0
    ignored = 0
    explicit_title_qualifier_by_textage_id: dict[str, str] = {}

    for tag, row in titletbl.items():
        if tag not in datatbl or tag not in actbl:
            ignored += 1
            continue

        version_raw = str(row[0])
        version = "SS" if version_raw == "-35" else version_raw
        textage_id = str(row[1])

        genre = normalize_textage_string(row[3])
        artist = normalize_textage_string(row[4])
        title = normalize_textage_string(row[5])

        if len(row) > 6 and row[6]:
            subtitle = normalize_textage_string(row[6])
            if subtitle:
                title = f"{title} {subtitle}"

        act_row = actbl[tag]
        value = act_row[0]
        flags = value if isinstance(value, int) else int(value, 16)
        is_ac_active = 1 if (flags & 0x01) else 0
        is_inf_active = 1 if (flags & 0x02) else 0

        music_id = upsert_music(
            conn,
            textage_id=textage_id,
            version=version,
            title=title,
            artist=artist,
            genre=genre,
            is_ac_active=is_ac_active,
            is_inf_active=is_inf_active,
        )
        explicit_qualifier = _extract_actbl_title_qualifier(act_row)
        if explicit_qualifier:
            explicit_title_qualifier_by_textage_id[textage_id] = explicit_qualifier
        music_processed += 1

        for chart_type, play_style, difficulty, _ in CHART_TYPES:
            notes = datatbl[tag][chart_type]
            lv_hex = act_row[chart_type * 2 + 1]
            lv_int = lv_hex if isinstance(lv_hex, int) else int(str(lv_hex), 16)
            is_active = 1 if lv_int > 0 else 0

            upsert_chart(
                conn=conn,
                music_id=music_id,
                play_style=play_style,
                difficulty=difficulty,
                level=lv_int,
                notes=int(notes),
                is_active=is_active,
            )
            chart_processed += 1

    resolve_music_title_qualifiers(
        conn=conn,
        explicit_title_qualifier_by_textage_id=explicit_title_qualifier_by_textage_id,
    )

    alias_report = rebuild_music_title_aliases(
        conn=conn,
        bemaniwiki_alias_config=bemaniwiki_alias_config,
    )

    asset_value = asset_updated_at or now_iso()
    upsert_meta(
        conn,
        schema_version=schema_version,
        asset_updated_at=asset_value,
        generated_at=now_iso(),
    )

    conn.commit()
    conn.close()

    return {
        "music_processed": music_processed,
        "chart_processed": chart_processed,
        "ignored": ignored,
        "official_alias_count": alias_report["official_alias_count"],
        "inserted_csv_wiki_alias_count": alias_report["inserted_csv_wiki_alias_count"],
        "unresolved_official_titles_count": alias_report[
            "unresolved_official_titles_count"
        ],
    }
