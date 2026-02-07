"""
曲マスタDB生成のエントリーポイント。

処理概要:
- settings.yaml を読み込み設定を取得
- 新曲/旧曲ページをスクレイピングし、対象tableを抽出して曲情報をパース
- 新旧を結合し、music_key重複を検知（重複があれば失敗）
- SQLiteへ反映（全件論理削除 → upsert）
- active件数の増減率チェック（90%未満なら失敗）
- GitHub Releases の latest に DB を asset としてアップロード（任意）
- Discordへ結果通知（任意、環境変数指定）

環境変数:
- DISCORD_WEBHOOK_URL: Discord通知先Webhook URL（未設定なら通知しない）
- GITHUB_TOKEN: GitHub Releases操作用トークン（upload_to_release有効時に必須）
"""

from __future__ import annotations

import os
import traceback

from src.config import load_settings
from src.db import (
    apply_song,
    build_music_key,
    connect_db,
    deactivate_all,
    get_new_active_count,
    get_prev_active_count,
    init_schema,
)
from src.discord_notify import send_discord
from src.errors import ValidationError
from src.github_release import delete_asset_if_exists, get_or_create_latest_release, upload_asset
from src.parser import find_target_table, parse_song_table
from src.scraper import fetch_html


def run() -> None:
    """
    曲マスタDB生成処理を実行する。

    Raises:
        ValidationError: データ整合性チェックや必須設定不足がある場合。
        Exception: 予期しないエラーが発生した場合。
    """
    settings = load_settings("settings.yaml")

    discord_webhook_url = (os.environ.get("DISCORD_WEBHOOK_URL") or "").strip() or None

    send_discord(discord_webhook_url, f"[START] song master build v{settings.version}")

    # スクレイピング
    new_html = fetch_html(settings.new_song_url)
    old_html = fetch_html(settings.old_song_url)

    new_idx, new_table = find_target_table(new_html)
    old_idx, old_table = find_target_table(old_html)

    new_rows = parse_song_table(new_table)
    old_rows = parse_song_table(old_table)

    merged_rows = old_rows + new_rows

    # 重複排除ルール: 同一music_keyが複数行なら先勝ちで後続を無視
    seen: dict[str, object] = {}
    deduplicated_rows = []
    for song in merged_rows:
        key = build_music_key(song.title, song.artist)
        if key not in seen:
            seen[key] = song
            deduplicated_rows.append(song)
    
    merged_rows = deduplicated_rows

    con = connect_db(settings.output_db_path)
    init_schema(con)

    prev_active = get_prev_active_count(con)

    try:
        con.execute("BEGIN")
        deactivate_all(con)

        for song in merged_rows:
            apply_song(con, song)

        con.commit()

    except Exception as e:
        con.rollback()
        raise e

    new_active = get_new_active_count(con)

    # 増減率チェック
    if prev_active > 0 and new_active < int(prev_active * 0.9):
        raise ValidationError(
            f"Active count dropped too much: prev={prev_active}, new={new_active}"
        )

    # GitHub Releases upload
    if settings.github.upload_to_release:
        token = (os.environ.get("GITHUB_TOKEN") or "").strip()
        if not token:
            raise ValidationError(
                "GITHUB_TOKEN is empty but upload_to_release is enabled"
            )

        if not settings.github.owner or not settings.github.repo:
            raise ValidationError("github.owner/repo is empty")

        release = get_or_create_latest_release(
            settings.github.owner,
            settings.github.repo,
            token,
            tag_name=f"v{settings.version}",
        )
        delete_asset_if_exists(release, settings.github.asset_name, token)

        upload_url = release["upload_url"]
        upload_asset(upload_url, settings.github.asset_name, settings.output_db_path, token)

    msg = (
        f"[SUCCESS] v{settings.version}\n"
        f"new_table_index={new_idx}, old_table_index={old_idx}\n"
        f"rows(new)={len(new_rows)}, rows(old)={len(old_rows)}, merged={len(merged_rows)}\n"
        f"active_music_count={new_active}"
    )
    send_discord(discord_webhook_url, msg)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        tb = traceback.format_exc()
        print(tb)

        webhook_url = (os.environ.get("DISCORD_WEBHOOK_URL") or "").strip() or None
        send_discord(webhook_url, f"[FAILED]\n{e}\n```{tb}```")

        raise
