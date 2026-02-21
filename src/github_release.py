"""Helpers for GitHub Releases fetch/create/upload operations."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import requests

GITHUB_API = "https://api.github.com"


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }


def get_latest_release(repo: str, token: str) -> dict | None:
    """Return the latest published release JSON, or None when not found."""
    url = f"{GITHUB_API}/repos/{repo}/releases/latest"
    response = requests.get(url, headers=_headers(token), timeout=30)

    if response.status_code == 404:
        return None

    response.raise_for_status()
    return response.json()


def get_release_by_tag(repo: str, token: str, tag_name: str) -> dict | None:
    """Return release JSON for a tag, or None when the tag release is missing."""
    url = f"{GITHUB_API}/repos/{repo}/releases/tags/{tag_name}"
    response = requests.get(url, headers=_headers(token), timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def create_release(
    repo: str,
    token: str,
    tag_name: str,
    draft: bool = True,
    body: str | None = None,
) -> dict:
    """Create a release for `tag_name` and return the release JSON."""
    url = f"{GITHUB_API}/repos/{repo}/releases"
    payload = {
        "tag_name": tag_name,
        "name": tag_name,
        "draft": draft,
        "prerelease": False,
        "generate_release_notes": False,
    }
    if body is not None:
        payload["body"] = body

    response = requests.post(url, headers=_headers(token), json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def _is_tag_already_exists(response: requests.Response) -> bool:
    if response.status_code != 422:
        return False
    try:
        payload = response.json()
    except ValueError:
        return "already_exists" in (response.text or "")

    for error in payload.get("errors", []):
        if isinstance(error, dict):
            if error.get("code") == "already_exists" and error.get("field") == "tag_name":
                return True

    return "already_exists" in str(payload)


def _iter_date_tag_candidates(base_date_tag: str, max_suffix: int) -> list[str]:
    if max_suffix < 1:
        raise ValueError("max_suffix must be >= 1")
    tags = [base_date_tag]
    for suffix in range(2, max_suffix + 1):
        tags.append(f"{base_date_tag}.{suffix}")
    return tags


def _resolve_base_date_tag(generated_at: str | None = None) -> str:
    if generated_at:
        parsed = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        return parsed.date().isoformat()
    return datetime.now(timezone.utc).date().isoformat()


def create_date_tag_release(
    repo: str,
    token: str,
    generated_at: str | None = None,
    max_suffix: int = 200,
    draft: bool = True,
    release_body_template: str | None = None,
) -> dict:
    """
    Create a new immutable date-tag release.

    Tag sequence:
    - first attempt: YYYY-MM-DD
    - if exists: YYYY-MM-DD.2, YYYY-MM-DD.3, ...
    """
    base_date_tag = _resolve_base_date_tag(generated_at=generated_at)
    last_error = None
    for tag_name in _iter_date_tag_candidates(base_date_tag, max_suffix=max_suffix):
        try:
            body = None
            if release_body_template is not None:
                body = release_body_template.format(tag=tag_name)
            release = create_release(
                repo=repo,
                token=token,
                tag_name=tag_name,
                draft=draft,
                body=body,
            )
            release["tag_name"] = tag_name
            return release
        except requests.HTTPError as exc:
            response = exc.response
            if response is not None and _is_tag_already_exists(response):
                last_error = exc
                continue
            raise

    raise RuntimeError(
        f"failed to create unique date tag release for {base_date_tag} up to .{max_suffix}"
    ) from last_error


def find_asset_by_name(release: dict, asset_name: str) -> dict | None:
    """Find one release asset by exact `asset_name`."""
    for asset in release.get("assets", []):
        if asset.get("name") == asset_name:
            return asset
    return None


def delete_asset(repo: str, token: str, asset_id: int):
    """Delete one release asset by asset id."""
    url = f"{GITHUB_API}/repos/{repo}/releases/assets/{asset_id}"
    response = requests.delete(url, headers=_headers(token), timeout=30)
    response.raise_for_status()


def download_asset(asset: dict, output_path: str, token: str | None = None):
    """Download a release asset to `output_path`."""
    download_url = asset.get("browser_download_url")
    if not download_url:
        raise RuntimeError("release asset missing browser_download_url")

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(download_url, headers=headers, timeout=60)
    response.raise_for_status()

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as file_obj:
        file_obj.write(response.content)


def upload_asset(upload_url_template: str, token: str, filepath: str, name: str):
    """Upload one file to a release upload URL."""
    upload_url = upload_url_template.split("{")[0] + f"?name={name}"

    with open(filepath, "rb") as file_obj:
        data = file_obj.read()

    headers = _headers(token)
    headers["Content-Type"] = "application/octet-stream"

    response = requests.post(upload_url, headers=headers, data=data, timeout=60)
    response.raise_for_status()
    return response.json()


def upload_files_to_release(release: dict, token: str, file_paths: list[str]):
    """
    Upload files as assets to a specific release.

    This function never deletes/replaces existing assets.
    """
    asset_names = [os.path.basename(path) for path in file_paths]
    if len(asset_names) != len(set(asset_names)):
        raise ValueError("duplicate asset file names in upload input")

    for file_path in file_paths:
        upload_asset(
            upload_url_template=release["upload_url"],
            token=token,
            filepath=file_path,
            name=os.path.basename(file_path),
        )


def publish_files_as_new_date_release(
    repo: str,
    token: str,
    file_paths: list[str],
    generated_at: str | None = None,
    max_suffix: int = 200,
    draft: bool = True,
    release_body_template: str | None = None,
) -> dict:
    """
    Create a new date-tag release and upload files to it.

    Returns created release JSON.
    """
    release = create_date_tag_release(
        repo=repo,
        token=token,
        generated_at=generated_at,
        max_suffix=max_suffix,
        draft=draft,
        release_body_template=release_body_template,
    )
    upload_files_to_release(release=release, token=token, file_paths=file_paths)
    return release


def upload_files_to_latest_release(*_args, **_kwargs):
    raise RuntimeError(
        "latest-tag publish is disabled. use publish_files_as_new_date_release()."
    )


def upload_sqlite_to_latest_release(*_args, **_kwargs):
    raise RuntimeError(
        "latest-tag publish is disabled. use publish_files_as_new_date_release()."
    )
