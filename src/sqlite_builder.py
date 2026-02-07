"""
SQLiteデータベース(song_master.sqlite)の構築と更新を行うモジュール。

Textageの以下3ファイルから取得したデータを元に、SQLiteへ登録・更新を行う。

- titletbl.js : 曲情報（タイトル/アーティスト/ジャンル/version/textage_id）
- datatbl.js  : ノーツ数情報
- actbl.js    : 譜面レベル情報、AC/INFINITAS収録フラグ

本モジュールは、曲情報(musicテーブル)と譜面情報(chartテーブル)を
Upsert(存在すれば更新、無ければ追加)することでDBを最新状態に保つ。

追加仕様:
- latest release の sqlite をDLして利用可能にする
- 更新開始前に music.is_ac_active / is_inf_active を全件0にリセットし、
  Textageで取得できた曲のみ再度フラグを立てる（Textageに無い曲は未収録扱い）

想定仕様:
- musicの同一判定は textage_id を用いる
- chartの同一判定は (music_id, play_style, difficulty) を用いる
- actblの譜面レベル値は16進数文字列であり、intに変換して保持する
"""

import sqlite3
import re
import html
import os
import requests
from datetime import datetime, timezone


TAG_RE = re.compile(r"<[^>]+>")


def normalize_textage_string(s: str) -> str:
    """
    Textage由来の文字列をDB登録用に正規化する。

    対応内容:
    - HTML文字実体参照のデコード (例: &#332; -> Ō)
    - HTMLタグ除去 (例: <br>, <span ...> 等)
    - 空白の正規化
    """
    if s is None:
        return ""

    s = str(s)

    # 文字実体参照をデコード
    s = html.unescape(s)

    # HTMLタグ除去
    s = TAG_RE.sub("", s)

    # 空白正規化
    s = re.sub(r"\s+", " ", s).strip()

    return s


def now_iso() -> str:
    """現在のUTC時刻をISO 8601形式で返す。"""
    return datetime.now(timezone.utc).isoformat()


# chart登録対象の譜面種別一覧。
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


def download_latest_sqlite_from_release(
    owner: str,
    repo: str,
    sqlite_path: str,
    token: str | None = None,
    asset_name: str = "song_master.sqlite",
) -> bool:
    """
    GitHub Releases の latest release から sqlite asset をダウンロードする。

    Returns:
        bool: ダウンロード成功ならTrue。latestが無い/assetが無い場合はFalse。
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"

    headers = {
        "Accept": "application/vnd.github+json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    r = requests.get(url, headers=headers, timeout=30)

    # releaseが無い場合
    if r.status_code == 404:
        return False

    r.raise_for_status()
    data = r.json()

    assets = data.get("assets", [])
    target = None
    for a in assets:
        if a.get("name") == asset_name:
            target = a
            break

    if not target:
        return False

    download_url = target.get("browser_download_url")
    if not download_url:
        return False

    r2 = requests.get(download_url, timeout=60)
    r2.raise_for_status()

    os.makedirs(os.path.dirname(sqlite_path) or ".", exist_ok=True)
    with open(sqlite_path, "wb") as f:
        f.write(r2.content)

    return True


def ensure_schema(conn: sqlite3.Connection):
    """DBスキーマを初期化する。"""
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS music (
        music_id INTEGER PRIMARY KEY AUTOINCREMENT,
        textage_id TEXT NOT NULL UNIQUE,
        version TEXT NOT NULL,
        title TEXT NOT NULL,
        artist TEXT NOT NULL,
        genre TEXT NOT NULL,
        is_ac_active INTEGER NOT NULL,
        is_inf_active INTEGER NOT NULL,
        last_seen_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """)

    cur.execute("""
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
    """)
    
    # インデックス追加: chart(music_id, is_active)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chart_music_active ON chart(music_id, is_active);")

    # インデックス追加: chart(play_style, difficulty, level, is_active)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chart_filter ON chart(play_style, difficulty, level, is_active);")

    # インデックス追加: chart(is_active, notes)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chart_notes_active ON chart(is_active, notes);")
    
    conn.commit()


def reset_all_music_active_flags(conn: sqlite3.Connection):
    """
    musicテーブルの収録フラグを全件0に戻す。

    Textage取得結果に無い曲は未収録扱いにしたいので、
    build開始前に必ず呼ぶ。
    """
    cur = conn.cursor()
    now = now_iso()

    cur.execute("""
    UPDATE music SET
        is_ac_active = 0,
        is_inf_active = 0,
        updated_at = ?
    """, (now,))

    conn.commit()


def upsert_music(
    conn: sqlite3.Connection,
    textage_id: str,
    version: str,
    title: str,
    artist: str,
    genre: str,
    is_ac_active: int,
    is_inf_active: int
) -> int:
    """musicテーブルに対してUpsertを行う。"""
    cur = conn.cursor()
    now = now_iso()

    cur.execute("SELECT music_id, created_at FROM music WHERE textage_id = ?", (textage_id,))
    row = cur.fetchone()

    if row is None:
        cur.execute("""
        INSERT INTO music (
            textage_id, version, title, artist, genre,
            is_ac_active, is_inf_active,
            last_seen_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            textage_id, version, title, artist, genre,
            is_ac_active, is_inf_active,
            now, now, now
        ))
        return cur.lastrowid

    music_id = row[0]
    cur.execute("""
    UPDATE music SET
        version = ?,
        title = ?,
        artist = ?,
        genre = ?,
        is_ac_active = ?,
        is_inf_active = ?,
        last_seen_at = ?,
        updated_at = ?
    WHERE textage_id = ?
    """, (
        version, title, artist, genre,
        is_ac_active, is_inf_active,
        now, now,
        textage_id
    ))
    return music_id


def upsert_chart(
    conn: sqlite3.Connection,
    music_id: int,
    play_style: str,
    difficulty: str,
    level: int,
    notes: int,
    is_active: int
):
    """chartテーブルに対してUpsertを行う。"""
    cur = conn.cursor()
    now = now_iso()

    cur.execute("""
    SELECT chart_id FROM chart
    WHERE music_id = ? AND play_style = ? AND difficulty = ?
    """, (music_id, play_style, difficulty))
    row = cur.fetchone()

    if row is None:
        cur.execute("""
        INSERT INTO chart (
            music_id, play_style, difficulty,
            level, notes, is_active,
            last_seen_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            music_id, play_style, difficulty,
            level, notes, is_active,
            now, now, now
        ))
        return

    cur.execute("""
    UPDATE chart SET
        level = ?,
        notes = ?,
        is_active = ?,
        last_seen_at = ?,
        updated_at = ?
    WHERE music_id = ? AND play_style = ? AND difficulty = ?
    """, (
        level, notes, is_active,
        now, now,
        music_id, play_style, difficulty
    ))


def build_or_update_sqlite(
    sqlite_path: str,
    titletbl: dict,
    datatbl: dict,
    actbl: dict,
    reset_flags: bool = True
) -> dict:
    """
    Textageデータを元にSQLite DBを生成または更新する。

    reset_flags=True の場合、build開始前に
    music.is_ac_active / is_inf_active を全件0にする。
    """
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    ensure_schema(conn)

    if reset_flags:
        reset_all_music_active_flags(conn)

    music_processed = 0
    chart_processed = 0
    ignored = 0

    for tag, row in titletbl.items():
        if tag not in datatbl or tag not in actbl:
            ignored += 1
            continue

        version_raw = str(row[0])

        # versionの -35 → SS は適用済み
        if version_raw == "-35":
            version = "SS"
        else:
            version = version_raw

        textage_id = str(row[1])

        genre = normalize_textage_string(row[3])
        artist = normalize_textage_string(row[4])
        title = normalize_textage_string(row[5])

        if len(row) > 6 and row[6]:
            subtitle = normalize_textage_string(row[6])
            if subtitle:
                title = f"{title} {subtitle}"

        value = actbl[tag][0]
        if isinstance(value, int):
            flags = value
        else:
            flags = int(value, 16)

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
            is_inf_active=is_inf_active
        )
        music_processed += 1

        for t, play_style, difficulty, _ in CHART_TYPES:
            notes = datatbl[tag][t]
            lv_hex = actbl[tag][t * 2 + 1]

            if isinstance(lv_hex, int):
                lv_int = lv_hex
            else:
                lv_int = int(str(lv_hex), 16)

            is_active = 1 if lv_int > 0 else 0

            upsert_chart(
                conn,
                music_id=music_id,
                play_style=play_style,
                difficulty=difficulty,
                level=lv_int,
                notes=int(notes),
                is_active=is_active
            )
            chart_processed += 1

    conn.commit()
    conn.close()

    return {
        "music_processed": music_processed,
        "chart_processed": chart_processed,
        "ignored": ignored,
    }
