"""Fetch and decode BEMANIWiki HTML for title alias extraction."""

from __future__ import annotations

import codecs
import os
import re
from dataclasses import dataclass
from pathlib import Path

import requests

from src.config import BemaniWikiAliasConfig

CONTENT_TYPE_CHARSET_RE = re.compile(r"charset\s*=\s*([A-Za-z0-9._-]+)", flags=re.I)
META_CHARSET_RE = re.compile(
    r"<meta[^>]+charset\s*=\s*['\"]?\s*([A-Za-z0-9._-]+)",
    flags=re.I,
)
META_CONTENT_TYPE_RE = re.compile(
    r"<meta[^>]+content\s*=\s*['\"][^'\"]*charset\s*=\s*([A-Za-z0-9._-]+)",
    flags=re.I,
)


@dataclass(frozen=True)
class BemaniWikiHtmlDocument:
    """Decoded BEMANIWiki document and metadata."""

    html_text: str
    encoding: str
    source: str
    replacement_char_count: int


def _normalize_encoding_name(name: str | None) -> str | None:
    if not name:
        return None
    try:
        return codecs.lookup(name.strip()).name
    except LookupError:
        return None


def _charset_from_content_type(content_type: str | None) -> str | None:
    if not content_type:
        return None
    match = CONTENT_TYPE_CHARSET_RE.search(content_type)
    if not match:
        return None
    return _normalize_encoding_name(match.group(1))


def _charset_from_html_meta(raw_html: bytes) -> str | None:
    head = raw_html[:16384].decode("ascii", errors="ignore")
    for pattern in (META_CHARSET_RE, META_CONTENT_TYPE_RE):
        match = pattern.search(head)
        if match:
            normalized = _normalize_encoding_name(match.group(1))
            if normalized:
                return normalized
    return None


def _charset_by_estimation(raw_html: bytes) -> str | None:
    try:
        from charset_normalizer import from_bytes as detect_charset

        guessed = detect_charset(raw_html).best()
        if guessed and guessed.encoding:
            normalized = _normalize_encoding_name(guessed.encoding)
            if normalized:
                return normalized
    except (ImportError, AttributeError):
        pass

    try:
        import chardet  # type: ignore

        detected = chardet.detect(raw_html)
        if detected and detected.get("encoding"):
            normalized = _normalize_encoding_name(str(detected["encoding"]))
            if normalized:
                return normalized
    except ImportError:
        pass

    return None


def _decode_html_bytes(raw_html: bytes, header_charset: str | None) -> tuple[str, str, int]:
    selected_encoding = (
        _normalize_encoding_name(header_charset)
        or _charset_from_html_meta(raw_html)
        or _charset_by_estimation(raw_html)
        or "utf-8"
    )
    decoded = raw_html.decode(selected_encoding, errors="replace")
    replacement_count = decoded.count("\ufffd")
    if replacement_count >= max(32, len(decoded) // 500):
        print(
            "[alias/wiki] warning: large decode replacement count "
            f"(encoding={selected_encoding}, replacements={replacement_count})"
        )
    return decoded, selected_encoding, replacement_count


def _save_cache(cache_path: str | None, payload: bytes):
    if not cache_path:
        return
    path = Path(cache_path)
    os.makedirs(path.parent, exist_ok=True)
    path.write_bytes(payload)


def _load_cache(cache_path: str | None) -> bytes | None:
    if not cache_path:
        return None
    path = Path(cache_path)
    if not path.exists():
        return None
    return path.read_bytes()


def _fetch_online(config: BemaniWikiAliasConfig) -> tuple[bytes, str | None]:
    headers = {}
    if config.user_agent:
        headers["User-Agent"] = config.user_agent

    response = requests.get(
        config.title_alias_url,
        headers=headers,
        timeout=config.http_timeout_sec,
    )
    response.raise_for_status()
    return response.content, _charset_from_content_type(response.headers.get("Content-Type"))


def load_bemaniwiki_title_alias_html(config: BemaniWikiAliasConfig) -> BemaniWikiHtmlDocument:
    """Load BEMANIWiki HTML from online or local fixture source."""
    raw_html: bytes
    header_charset: str | None = None
    source = config.source_mode

    if config.source_mode == "file":
        assert config.source_file_path is not None
        raw_html = Path(config.source_file_path).read_bytes()
        source = f"file:{config.source_file_path}"
    else:
        try:
            raw_html, header_charset = _fetch_online(config)
            source = f"online:{config.title_alias_url}"
            _save_cache(config.cache_path, raw_html)
        except requests.RequestException:
            if config.online_failure_mode != "cache_fallback":
                raise
            cached = _load_cache(config.cache_path)
            if cached is None:
                raise
            raw_html = cached
            source = f"cache:{config.cache_path}"

    html_text, encoding, replacement_count = _decode_html_bytes(raw_html, header_charset)
    return BemaniWikiHtmlDocument(
        html_text=html_text,
        encoding=encoding,
        source=source,
        replacement_char_count=replacement_count,
    )
