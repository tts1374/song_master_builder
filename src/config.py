"""Configuration helpers for optional title alias ingestion."""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_BEMANIWIKI_TITLE_ALIAS_URL = (
    "https://bemaniwiki.com/?beatmania+IIDX+33+Sparkle+Shower/"
    "%B6%CA%CC%BE%C9%BD%B5%AD%A4%CB%A4%C4%A4%A4%A4%C6"
)


@dataclass(frozen=True)
class BemaniWikiAliasConfig:
    """Runtime settings used to load BEMANIWiki title-alias conversion table."""

    title_alias_url: str
    http_timeout_sec: int
    user_agent: str | None
    cache_path: str | None
    source_mode: str
    source_file_path: str | None
    online_failure_mode: str
    unresolved_official_title_fail_threshold: int | None


def _as_int(raw_value, default_value: int) -> int:
    if raw_value is None:
        return default_value
    return int(raw_value)


def load_bemaniwiki_alias_config(settings: dict) -> BemaniWikiAliasConfig:
    """Read BEMANIWiki alias ingestion settings from top-level YAML settings."""
    source_mode = str(settings.get("bemaniwiki_source_mode", "online")).strip().lower()
    if source_mode not in {"online", "file"}:
        raise ValueError("bemaniwiki_source_mode must be 'online' or 'file'")

    online_failure_mode = (
        str(settings.get("bemaniwiki_online_failure_mode", "fail_fast")).strip().lower()
    )
    if online_failure_mode not in {"fail_fast", "cache_fallback"}:
        raise ValueError(
            "bemaniwiki_online_failure_mode must be 'fail_fast' or 'cache_fallback'"
        )

    source_file_path_raw = settings.get("bemaniwiki_source_file_path")
    source_file_path = (
        str(source_file_path_raw).strip() if source_file_path_raw is not None else None
    )
    if source_mode == "file" and not source_file_path:
        raise ValueError(
            "bemaniwiki_source_mode='file' requires bemaniwiki_source_file_path"
        )

    threshold_raw = settings.get("bemaniwiki_unresolved_official_title_fail_threshold")
    unresolved_threshold = None
    if threshold_raw is not None and str(threshold_raw).strip() != "":
        unresolved_threshold = int(threshold_raw)
        if unresolved_threshold < 0:
            raise ValueError(
                "bemaniwiki_unresolved_official_title_fail_threshold must be >= 0"
            )

    cache_path_raw = settings.get("bemaniwiki_cache_path")
    cache_path = str(cache_path_raw).strip() if cache_path_raw else None

    user_agent_raw = settings.get("bemaniwiki_user_agent")
    user_agent = str(user_agent_raw).strip() if user_agent_raw else None

    return BemaniWikiAliasConfig(
        title_alias_url=str(
            settings.get(
                "bemaniwiki_title_alias_url", DEFAULT_BEMANIWIKI_TITLE_ALIAS_URL
            )
        ).strip(),
        http_timeout_sec=_as_int(settings.get("bemaniwiki_http_timeout_sec"), 20),
        user_agent=user_agent,
        cache_path=cache_path,
        source_mode=source_mode,
        source_file_path=source_file_path,
        online_failure_mode=online_failure_mode,
        unresolved_official_title_fail_threshold=unresolved_threshold,
    )
