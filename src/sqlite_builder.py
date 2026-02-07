"""
SQLiteデータベース(song_master.sqlite)の構築と更新を行うモジュール。

Textageの以下3ファイルから取得したデータを元に、SQLiteへ登録・更新を行う。

- titletbl.js : 曲情報（タイトル/アーティスト/ジャンル/version/textage_id）
- datatbl.js  : ノーツ数情報
- actbl.js    : 譜面レベル情報、AC/INFINITAS収録フラグ

本モジュールは、曲情報(musicテーブル)と譜面情報(chartテーブル)を
Upsert(存在すれば更新、無ければ追加)することでDBを最新状態に保つ。

想定仕様:
- musicの同一判定は textage_id を用いる
- chartの同一判定は (music_id, play_style, difficulty) を用いる
- actblの譜面レベル値は16進数文字列であり、intに変換して保持する
"""

import sqlite3
from datetime import datetime, timezone


def now_iso() -> str:
    """
    現在のUTC時刻をISO 8601形式の文字列で返す。

    Returns:
        str: ISO 8601形式でフォーマットされた現在のUTC時刻
    """
    return datetime.now(timezone.utc).isoformat()


# chart登録対象の譜面種別一覧。
# typeは textage(datatable/actbl) のインデックスに対応する。
# act_indexはactbl上の該当レベル位置(参考情報、コード上は type*2+1 を使用する)。
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


def ensure_schema(conn: sqlite3.Connection):
    """
    データベーススキーマを初期化する関数。

    musicテーブルとchartテーブルを作成し、データベースの構造を整える。
    既にテーブルが存在する場合は作成処理はスキップされる。

    Args:
        conn (sqlite3.Connection): SQLiteデータベース接続オブジェクト
    """
    cur = conn.cursor()

    # 曲情報テーブル
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

    # 譜面情報テーブル
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
    """
    musicテーブルに対してUpsertを行う。

    textage_id を一意キーとして、存在しなければINSERT、存在すればUPDATEする。
    created_at は新規登録時のみ設定し、更新時は保持する。

    Args:
        conn (sqlite3.Connection): SQLiteデータベース接続
        textage_id (str): Textage恒久ID（同一判定キー）
        version (str): バージョン番号（例: "33"）
        title (str): タイトル
        artist (str): アーティスト
        genre (str): ジャンル
        is_ac_active (int): AC収録フラグ (0/1)
        is_inf_active (int): INFINITAS収録フラグ (0/1)

    Returns:
        int: 対象musicのmusic_id
    """
    cur = conn.cursor()
    now = now_iso()

    # 既存データ検索
    cur.execute("SELECT music_id, created_at FROM music WHERE textage_id = ?", (textage_id,))
    row = cur.fetchone()

    # 新規登録
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

    # 更新
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
    """
    chartテーブルに対してUpsertを行う。

    (music_id, play_style, difficulty) を一意キーとして
    存在しなければINSERT、存在すればUPDATEする。

    Args:
        conn (sqlite3.Connection): SQLiteデータベース接続
        music_id (int): musicテーブルの内部ID
        play_style (str): SP/DP
        difficulty (str): BEGINNER/NORMAL/HYPER/ANOTHER/LEGGENDARIA
        level (int): 譜面レベル（数値）
        notes (int): ノーツ数
        is_active (int): 有効フラグ (0/1)
    """
    cur = conn.cursor()
    now = now_iso()

    cur.execute("""
    SELECT chart_id FROM chart
    WHERE music_id = ? AND play_style = ? AND difficulty = ?
    """, (music_id, play_style, difficulty))
    row = cur.fetchone()

    # 新規登録
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

    # 更新
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
    actbl: dict
) -> dict:
    """
    Textageデータを元にSQLite DBを生成または更新する。

    titletblを起点として全曲を走査し、datatbl/actblが揃っている曲のみを処理する。
    musicテーブルを更新後、CHART_TYPES定義に従いchartテーブルも更新する。

    Args:
        sqlite_path (str): SQLiteファイルパス
        titletbl (dict): titletbl.jsから抽出した辞書
        datatbl (dict): datatbl.jsから抽出した辞書
        actbl (dict): actbl.jsから抽出した辞書

    Returns:
        dict: 処理件数情報
            - music_processed: music処理件数
            - chart_processed: chart処理件数
            - ignored: datatbl/actbl不足による無視件数
    """
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    ensure_schema(conn)

    music_processed = 0
    chart_processed = 0
    ignored = 0

    for tag, row in titletbl.items():
        # datatbl/actblが無い曲は処理対象外とする
        if tag not in datatbl or tag not in actbl:
            ignored += 1
            continue

        # titletbl: [version, textage_id, opt?, genre, artist, title, subtitle?]
        version_raw = str(row[0])

        # textage_loader側で SS=35 を -35 に変換しているため、
        # -35 は SS として扱いDBへ登録する
        if version_raw == "-35":
            version = "SS"
        else:
            version = version_raw
        textage_id = str(row[1])
        genre = str(row[3])
        artist = str(row[4])
        title = str(row[5])
        if len(row) > 6 and row[6]:
            title = title + " " + str(row[6])

        # actbl[tag][0] はフラグ領域（16進数文字列または整数値）
        # bit0: AC収録
        # bit1: INFINITAS収録
        value = actbl[tag][0]
        if isinstance(value, int):
            flags = value
        else:
            flags = int(value, 16)
        is_ac_active = 1 if (flags & 0x01) else 0
        is_inf_active = 1 if (flags & 0x02) else 0

        # musicをUpsert
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

        # chartをUpsert
        for t, play_style, difficulty, _ in CHART_TYPES:
            # datatbl: ノーツ数
            notes = datatbl[tag][t]

            # actbl: 譜面レベル (16進数表記の文字列 or int)
            lv_hex = actbl[tag][t * 2 + 1]

            if isinstance(lv_hex, int):
                # 万が一intの場合はそのまま利用
                lv_int = lv_hex
            else:
                # "A" 等の16進数文字列を想定
                lv_int = int(str(lv_hex), 16)

            # レベルが0の場合は譜面無し扱い（無効）
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
