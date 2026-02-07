"""
IIDX楽曲マスターデータ(SQLite)を生成・更新し、GitHub Releasesへ配布するエントリーポイント。

本スクリプトは settings.yaml を読み込み、Textageから取得した最新データを用いて
SQLiteデータベース(song_master.sqlite)を更新する。

主な処理フロー:
1. settings.yaml 読み込み
2. GitHub Releases の latest から既存sqliteを取得（存在すれば）
3. Textageから最新テーブル(titletbl/datatbl/actbl)を取得
4. sqliteをUpsert更新（既存データとの整合性を維持）
5. 必要に応じてGitHub Releasesへアップロード
6. Discord Webhookへ成功/失敗通知

必要な環境変数:
- GITHUB_TOKEN: GitHub APIアクセス用トークン
- DISCORD_WEBHOOK_URL: Discord通知先（任意）
"""

import os
from datetime import datetime, timezone
import sys
import traceback
import yaml

from src.textage_loader import fetch_textage_tables
from src.sqlite_builder import build_or_update_sqlite, download_latest_sqlite_from_release
from src.github_release import upload_sqlite_to_latest_release
from src.discord_notify import send_discord_message


def now_iso() -> str:
    """
    現在のUTC時刻をISO 8601形式の文字列で取得する。

    Returns:
        str: ISO 8601形式でフォーマットされた現在のUTC時刻文字列。
    """
    return datetime.now(timezone.utc).isoformat()


def load_settings(path: str = "settings.yaml") -> dict:
    """
    YAML形式の設定ファイル(settings.yaml)を読み込み、辞書として返す。

    Args:
        path (str): 設定ファイルのパス。デフォルトは "settings.yaml"。

    Returns:
        dict: YAMLをパースした設定データ。
              読み込みに成功した場合、トップレベルはdictとなる。

    Raises:
        FileNotFoundError: 指定されたファイルが存在しない場合。
        yaml.YAMLError: YAMLとしてパースできない場合。
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    """
    song_master.sqlite を生成・更新し、GitHub Releasesへ配布するメイン処理。

    settings.yaml の内容に従い、以下を実行する:
    - GitHub Releases の latest release から既存sqliteをダウンロード（存在すれば）
    - Textageから最新テーブルデータを取得
    - SQLiteへUpsert更新（既存レコードを維持しつつ更新）
    - upload_to_release が true の場合は latest release へアップロード
    - Discord Webhook が指定されている場合は成功/失敗を通知

    settings.yaml の想定キー:
    - output_db_path: 出力sqliteのパス
    - github.owner: GitHubオーナー名
    - github.repo: GitHubリポジトリ名
    - github.upload_to_release: アップロード有無
    - github.asset_name: release asset 名（DL対象名）

    必要な環境変数:
    - GITHUB_TOKEN: GitHub APIアクセストークン
    - DISCORD_WEBHOOK_URL: Discord通知用Webhook URL（任意）

    Raises:
        Exception: 内部処理で例外が発生した場合はそのまま再送出する。
                   失敗時はDiscord通知（設定されていれば）を行う。
    """
    try:
        settings = load_settings("settings.yaml")

        sqlite_path = settings.get("output_db_path", "song_master.sqlite")

        github_cfg = settings.get("github", {})
        owner = github_cfg.get("owner")
        repo = github_cfg.get("repo")
        upload_to_release = github_cfg.get("upload_to_release", False)
        asset_name = github_cfg.get("asset_name", "song_master.sqlite")

        if not owner or not repo:
            raise RuntimeError("settings.yaml: github.owner / github.repo が未設定です")

        token = os.environ["GITHUB_TOKEN"]
        discord_webhook = os.environ.get("DISCORD_WEBHOOK_URL")

        # 0. latest release から sqlite を取得（あれば）
        downloaded = download_latest_sqlite_from_release(
            owner=owner,
            repo=repo,
            sqlite_path=sqlite_path,
            token=token,
            asset_name=asset_name,
        )

        # 1. textage JS取得
        titletbl, datatbl, actbl = fetch_textage_tables()

        # 2. SQLite更新
        result = build_or_update_sqlite(
            sqlite_path=sqlite_path,
            titletbl=titletbl,
            datatbl=datatbl,
            actbl=actbl,
            reset_flags=True,
        )

        # 3. GitHub Releasesへアップロード
        if upload_to_release:
            upload_sqlite_to_latest_release(
                repo=f"{owner}/{repo}",
                token=token,
                sqlite_path=sqlite_path
            )

        # 4. Discord通知（成功）
        if discord_webhook:
            msg = (
                f"✅ song_master.sqlite 更新成功\n"
                f"- downloaded_from_release: {downloaded}\n"
                f"- music processed: {result['music_processed']}\n"
                f"- chart processed: {result['chart_processed']}\n"
                f"- ignored: {result['ignored']}\n"
                f"- updated_at: {now_iso()}\n"
            )
            send_discord_message(discord_webhook, msg)

        print("SUCCESS")

    except Exception:
        err = traceback.format_exc()
        print(err, file=sys.stderr)

        discord_webhook = os.environ.get("DISCORD_WEBHOOK_URL")
        if discord_webhook:
            msg = (
                f"❌ song_master.sqlite 更新失敗\n"
                f"```{err[:1800]}```"
            )
            send_discord_message(discord_webhook, msg)

        raise


if __name__ == "__main__":
    main()
