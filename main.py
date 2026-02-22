# pylint: disable=duplicate-code
"""
SQLite 楽曲マスター成果物を生成し、GitHub Releases に配布するエントリーポイント。
"""

from __future__ import annotations

# pylint: disable=duplicate-code

import json
import os
import shutil
import sys
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from src.build_validation import (
    build_latest_manifest,
    validate_chart_id_stability,
    validate_db_schema_and_data,
    validate_latest_manifest,
    write_latest_manifest,
)
from src.discord_notify import send_discord_message
from src.github_release import (
    download_asset,
    find_asset_by_name,
    get_latest_release,
    publish_files_as_new_date_release,
)
from src.sqlite_builder import DEFAULT_MANUAL_ALIAS_CSV_PATH, build_or_update_sqlite
from src.textage_loader import fetch_textage_tables_with_hashes

JST = timezone(timedelta(hours=9), "JST")
LATEST_MANIFEST_NAME = "latest.json"


def now_iso() -> str:
    """現在時刻（JST）を ISO8601 文字列で返す。"""
    return datetime.now(JST).isoformat()


def load_settings(path: str = "settings.yaml") -> dict:
    """YAML設定ファイルを辞書として読み込む。"""
    with open(path, "r", encoding="utf-8") as file_obj:
        return yaml.safe_load(file_obj)


def parse_bool(value, default: bool = False) -> bool:
    """多様な入力値を bool に正規化する。"""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def has_same_textage_source_hashes(
    previous_hashes: dict[str, str] | None,
    current_hashes: dict[str, str],
) -> bool:
    """Textage 3ファイルのハッシュが全一致するか判定する。"""
    if not previous_hashes:
        return False

    required_keys = ("titletbl.js", "datatbl.js", "actbl.js")
    for key in required_keys:
        if previous_hashes.get(key) != current_hashes.get(key):
            return False
    return True


def resolve_artifact_paths(
    output_db_path: str,
    latest_manifest_name: str,
    generated_utc: datetime,
) -> dict:
    """バージョン付き SQLite 名と latest.json パスを解決する。"""
    output_base = Path(output_db_path)
    output_dir = output_base.parent if str(output_base.parent) not in {"", "."} else Path(".")
    stem = output_base.stem if output_base.suffix else output_base.name
    version = generated_utc.strftime("%Y-%m-%d")
    sqlite_file_name = f"{stem}_{version}.sqlite"

    sqlite_path = output_dir / sqlite_file_name
    latest_json_path = output_dir / latest_manifest_name

    return {
        "output_dir": str(output_dir),
        "sqlite_path": str(sqlite_path),
        "sqlite_file_name": sqlite_file_name,
        "latest_json_path": str(latest_json_path),
    }


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def download_previous_sqlite_from_release(
    repo_full: str,
    token: str,
    working_dir: str,
    latest_manifest_name: str,
    fallback_asset_name: str | None,
    required: bool,
) -> dict | None:
    """最新リリースから前回SQLiteを取得し、保存先メタを返す。"""
    release = get_latest_release(repo_full, token)
    if release is None:
        if required:
            raise RuntimeError("最新リリースが見つからず前回 SQLite を取得できません")
        return None

    sqlite_asset_name = None
    previous_manifest = None

    manifest_asset = find_asset_by_name(release, latest_manifest_name)
    if manifest_asset:
        manifest_path = os.path.join(working_dir, latest_manifest_name)
        download_asset(manifest_asset, manifest_path, token=token)
        with open(manifest_path, "r", encoding="utf-8") as file_obj:
            previous_manifest = json.load(file_obj)
        sqlite_asset_name = previous_manifest.get("file_name")

    if not sqlite_asset_name and fallback_asset_name:
        sqlite_asset_name = fallback_asset_name

    if not sqlite_asset_name:
        if required:
            raise RuntimeError(
                "最新リリースから前回 SQLite のアセット名を特定できません"
            )
        return None

    sqlite_asset = find_asset_by_name(release, sqlite_asset_name)
    if sqlite_asset is None:
        if required:
            raise RuntimeError(
                f"最新リリースに前回 SQLite アセットがありません: {sqlite_asset_name}"
            )
        return None

    previous_sqlite_path = os.path.join(working_dir, "previous_release.sqlite")
    download_asset(sqlite_asset, previous_sqlite_path, token=token)

    return {
        "sqlite_path": previous_sqlite_path,
        "asset_name": sqlite_asset_name,
        "asset_updated_at": sqlite_asset.get("updated_at"),
        "manifest": previous_manifest,
    }


def main():  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """生成・検証・公開までのビルドフローを実行する。"""
    try:
        settings = load_settings("settings.yaml")

        output_db_path = settings.get("output_db_path", "song_master.sqlite")
        schema_version = str(settings.get("schema_version", "1"))
        chart_id_missing_policy = str(settings.get("chart_id_missing_policy", "error"))
        manual_alias_csv_path = str(
            settings.get("music_alias_manual_csv_path", DEFAULT_MANUAL_ALIAS_CSV_PATH)
        ).strip() or DEFAULT_MANUAL_ALIAS_CSV_PATH

        github_cfg = settings.get("github", {})
        owner = github_cfg.get("owner")
        repo = github_cfg.get("repo")
        upload_to_release = parse_bool(github_cfg.get("upload_to_release", False))
        fallback_asset_name = github_cfg.get("asset_name", "song_master.sqlite")
        require_previous_release = parse_bool(
            github_cfg.get("require_previous_release"),
            default=parse_bool(os.environ.get("CI"), default=False),
        )

        if not owner or not repo:
            raise RuntimeError("settings.yaml: github.owner / github.repo は必須です")

        token = os.environ["GITHUB_TOKEN"]
        discord_webhook = os.environ.get("DISCORD_WEBHOOK_URL")
        repo_full = f"{owner}/{repo}"
        generated_utc = datetime.now(timezone.utc)
        generated_at = generated_utc.isoformat().replace("+00:00", "Z")

        artifacts = resolve_artifact_paths(
            output_db_path=output_db_path,
            latest_manifest_name=LATEST_MANIFEST_NAME,
            generated_utc=generated_utc,
        )
        sqlite_path = artifacts["sqlite_path"]
        latest_json_path = artifacts["latest_json_path"]

        os.makedirs(os.path.dirname(sqlite_path) or ".", exist_ok=True)

        with tempfile.TemporaryDirectory() as working_dir:
            previous_info = download_previous_sqlite_from_release(
                repo_full=repo_full,
                token=token,
                working_dir=working_dir,
                latest_manifest_name=LATEST_MANIFEST_NAME,
                fallback_asset_name=fallback_asset_name,
                required=require_previous_release,
            )

            previous_sqlite_path = None
            previous_asset_updated_at = None
            previous_source_hashes = None
            if previous_info:
                previous_sqlite_path = previous_info["sqlite_path"]
                previous_asset_updated_at = previous_info["asset_updated_at"]
                previous_manifest = previous_info.get("manifest")
                if isinstance(previous_manifest, dict):
                    source_hashes = previous_manifest.get("source_hashes")
                    if isinstance(source_hashes, dict):
                        previous_source_hashes = source_hashes

            titletbl, datatbl, actbl, source_hashes = fetch_textage_tables_with_hashes()

            if has_same_textage_source_hashes(previous_source_hashes, source_hashes):
                if discord_webhook:
                    send_discord_message(
                        discord_webhook,
                        "\n".join(
                            [
                                "song master build をスキップしました",
                                "- 理由: Textage ソースハッシュが未変更",
                                f"- 時刻: {now_iso()}",
                            ]
                        ),
                    )
                print("SKIPPED: Textage ソースハッシュ未変更")
                return

            if previous_info:
                shutil.copyfile(previous_sqlite_path, sqlite_path)
            elif os.path.exists(sqlite_path):
                os.remove(sqlite_path)

            result = build_or_update_sqlite(
                sqlite_path=sqlite_path,
                titletbl=titletbl,
                datatbl=datatbl,
                actbl=actbl,
                reset_flags=True,
                schema_version=schema_version,
                asset_updated_at=previous_asset_updated_at,
                manual_alias_csv_path=manual_alias_csv_path,
            )

            validate_db_schema_and_data(
                sqlite_path,
                expected_schema_version=schema_version,
            )

            if not previous_sqlite_path:
                if require_previous_release:
                    raise RuntimeError(
                        "chart_id 検証には前回 SQLite が必要ですが取得できませんでした"
                    )
                chart_check = None
            else:
                chart_check = validate_chart_id_stability(
                    old_sqlite_path=previous_sqlite_path,
                    new_sqlite_path=sqlite_path,
                    missing_policy=chart_id_missing_policy,
                )

        manifest = build_latest_manifest(
            sqlite_path=sqlite_path,
            schema_version=schema_version,
            generated_at=generated_at,
            source_hashes=source_hashes,
        )
        write_latest_manifest(latest_json_path, manifest)
        validate_latest_manifest(latest_json_path, sqlite_path)

        published_release = None
        if upload_to_release:
            published_release = publish_files_as_new_date_release(
                repo=repo_full,
                token=token,
                file_paths=[sqlite_path, latest_json_path],
                generated_at=manifest.get("generated_at"),
            )

        if discord_webhook:
            msg_lines = [
                "song master build 成功",
                f"- sqlite_file: {os.path.basename(sqlite_path)}",
                f"- latest_manifest: {os.path.basename(latest_json_path)}",
                f"- music_processed: {result['music_processed']}",
                f"- chart_processed: {result['chart_processed']}",
                f"- ignored: {result['ignored']}",
                f"- official_alias_count: {result['official_alias_count']}",
                f"- manual_alias_count: {result['inserted_manual_alias_count']}",
                "- manual_alias_redundant_skipped_count: "
                f"{result['skipped_redundant_manual_alias_count']}",
                f"- chart_id_checked: {'yes' if chart_check else 'no'}",
                f"- generated_at: {manifest['generated_at']}",
                f"- sha256: {manifest['sha256']}",
                f"- updated_at: {now_iso()}",
            ]
            if chart_check:
                msg_lines.append(f"- shared_charts: {chart_check['shared_total']}")
                msg_lines.append(f"- missing_in_new: {chart_check['missing_in_new_total']}")
            if published_release:
                msg_lines.append(f"- tag: {published_release.get('tag_name')}")
                msg_lines.append(f"- release_url: {published_release.get('html_url')}")
            send_discord_message(discord_webhook, "\n".join(msg_lines))

        print("SUCCESS")

    except Exception:
        err = traceback.format_exc()
        print(err, file=sys.stderr)

        discord_webhook = os.environ.get("DISCORD_WEBHOOK_URL")
        if discord_webhook:
            send_discord_message(
                discord_webhook,
                f"song master build 失敗\n```{err[:1800]}```",
            )

        raise


if __name__ == "__main__":
    main()
