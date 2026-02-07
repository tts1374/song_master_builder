"""
GitHub Releases の操作を行うモジュール。

- latest release の取得
- 既存 asset の削除
- sqlite ファイル等の asset アップロード

GitHub Actions 上で GITHUB_TOKEN を利用して動作する想定。
"""

from __future__ import annotations

from typing import Any, Dict

import requests

from src.errors import GithubReleaseError


def _github_headers(token: str) -> Dict[str, str]:
    """
    GitHub REST API 呼び出し用の共通ヘッダを生成する。

    Args:
        token: GitHub API用トークン（GITHUB_TOKEN 等）

    Returns:
        GitHub API呼び出しに必要なHTTPヘッダ辞書。
    """
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

def delete_asset_if_exists(release: Dict[str, Any], asset_name: str, token: str) -> None:
    """
    Release内に同名のassetが存在する場合、それを削除する。

    Args:
        release: GitHub APIから取得したrelease情報
        asset_name: 削除対象のasset名
        token: GitHub API用トークン

    Raises:
        GithubReleaseError: asset削除URLが取得できない、または削除APIが失敗した場合。
    """
    assets = release.get("assets") or []
    for asset in assets:
        if asset.get("name") == asset_name:
            delete_url = asset.get("url")
            if not delete_url:
                raise GithubReleaseError("Asset delete url not found")

            r = requests.delete(delete_url, headers=_github_headers(token), timeout=30)
            if r.status_code != 204:
                raise GithubReleaseError(f"Failed to delete asset: {r.status_code} {r.text}")
            return


def upload_asset(upload_url: str, asset_name: str, file_path: str, token: str) -> None:
    """
    Releaseのupload_urlに対してassetファイルをアップロードする。

    Args:
        upload_url: release情報に含まれる upload_url（テンプレート形式）
        asset_name: アップロードするasset名
        file_path: アップロード対象ファイルパス
        token: GitHub API用トークン

    Raises:
        GithubReleaseError: assetアップロードが失敗した場合。
    """
    upload_url = upload_url.split("{")[0]
    url = f"{upload_url}?name={asset_name}"

    with open(file_path, "rb") as f:
        data = f.read()

    headers = _github_headers(token)
    headers["Content-Type"] = "application/octet-stream"

    r = requests.post(url, headers=headers, data=data, timeout=60)
    if r.status_code not in (200, 201):
        raise GithubReleaseError(f"Failed to upload asset: {r.status_code} {r.text}")

def create_release(owner: str, repo: str, token: str, tag_name: str) -> Dict[str, Any]:
    """
    GitHub Release を新規作成する。

    Args:
        owner: GitHub owner名
        repo: GitHub repository名
        token: GitHub API用トークン
        tag_name: 作成するtag名（例: v1.0.0）

    Returns:
        作成されたreleaseのJSONレスポンス（dict）。

    Raises:
        GithubReleaseError: release作成に失敗した場合。
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/releases"

    payload = {
        "tag_name": tag_name,
        "name": tag_name,
        "draft": False,
        "prerelease": False,
        "generate_release_notes": False,
    }

    r = requests.post(url, headers=_github_headers(token), json=payload, timeout=30)
    if r.status_code not in (200, 201):
        raise GithubReleaseError(f"Failed to create release: {r.status_code} {r.text}")

    return r.json()


def get_or_create_latest_release(
    owner: str, repo: str, token: str, tag_name: str
) -> Dict[str, Any]:
    """
    latest release を取得する。存在しない(404)場合は新規作成する。

    Args:
        owner: GitHub owner名
        repo: GitHub repository名
        token: GitHub API用トークン
        tag_name: releaseが無い場合に作成するtag名

    Returns:
        latest release のJSONレスポンス（dict）。

    Raises:
        GithubReleaseError: 404以外で latest release 取得に失敗した場合。
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    r = requests.get(url, headers=_github_headers(token), timeout=30)

    if r.status_code == 200:
        return r.json()

    if r.status_code == 404:
        return create_release(owner, repo, token, tag_name)

    raise GithubReleaseError(f"Failed to get latest release: {r.status_code} {r.text}")