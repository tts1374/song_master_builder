"""AC score CSV import identification report and Discord notification."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

ALIAS_SCOPE_AC = "ac"
TITLE_COLUMN = "タイトル"
DISCORD_SAFE_LIMIT = 1900
UNMATCHED_TOP_N = 10

LOGGER = logging.getLogger(__name__)


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO8601 with Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_ac_alias_map(conn: sqlite3.Connection) -> dict[str, str]:
    """Load AC alias mapping once and return alias -> textage_id dict."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT alias, textage_id
        FROM music_title_alias
        WHERE alias_scope = ?
          AND alias_type IN ('official', 'manual')
        """,
        (ALIAS_SCOPE_AC,),
    )
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError(
            "music_title_alias alias_scope='ac' with alias_type in (official, manual) has no rows; "
            "run alias generation first"
        )

    return {str(alias): str(textage_id) for alias, textage_id in rows}


def _sorted_unmatched(counter: Counter[str]) -> list[tuple[str, int]]:
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))


def _read_csv_and_identify(
    csv_path: str,
    alias_map: dict[str, str],
) -> tuple[int, int, Counter[str]]:
    total_song_rows = 0
    matched_song_rows = 0
    unmatched_titles: Counter[str] = Counter()

    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as file_obj:
            reader = csv.DictReader(file_obj)
            if not reader.fieldnames or TITLE_COLUMN not in reader.fieldnames:
                raise RuntimeError(f"CSV missing required column: {TITLE_COLUMN}")

            for row in reader:
                total_song_rows += 1
                raw_title = row[TITLE_COLUMN]
                csv_title = str(raw_title).strip() if raw_title is not None else ""

                if alias_map.get(csv_title) is not None:
                    matched_song_rows += 1
                else:
                    unmatched_titles[csv_title] += 1
    except RuntimeError:
        raise
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise RuntimeError(f"Failed to read AC score CSV: {csv_path}") from exc

    return total_song_rows, matched_song_rows, unmatched_titles


def generate_import_report(
    source_csv_file: str,
    total_song_rows: int,
    matched_song_rows: int,
    unmatched_titles: Counter[str],
) -> dict:
    """Generate report payload for JSON and notification."""
    unmatched_song_rows = total_song_rows - matched_song_rows
    match_rate = 0.0
    if total_song_rows > 0:
        match_rate = (matched_song_rows / total_song_rows) * 100.0

    top_unmatched = [
        {"title": title, "count": count}
        for title, count in _sorted_unmatched(unmatched_titles)[:UNMATCHED_TOP_N]
    ]
    return {
        "source_csv_file": str(source_csv_file),
        "alias_scope": ALIAS_SCOPE_AC,
        "total_song_rows": int(total_song_rows),
        "matched_song_rows": int(matched_song_rows),
        "unmatched_song_rows": int(unmatched_song_rows),
        "match_rate": float(round(match_rate, 4)),
        "unmatched_titles_topN": top_unmatched,
        "generated_at": now_utc_iso(),
    }


def print_report_summary(report: dict) -> None:
    """Print concise summary to stdout."""
    print("AC score CSV identification report")
    print(f"- source_csv_file: {report['source_csv_file']}")
    print(f"- alias_scope: {report['alias_scope']}")
    print(f"- total_song_rows: {report['total_song_rows']}")
    print(f"- matched_song_rows: {report['matched_song_rows']}")
    print(f"- unmatched_song_rows: {report['unmatched_song_rows']}")
    print(f"- match_rate: {report['match_rate']:.2f}%")

    unmatched_top = report.get("unmatched_titles_topN", [])
    if not unmatched_top:
        print("- unmatched_titles_top10: None")
        return

    print("- unmatched_titles_top10:")
    for item in unmatched_top:
        print(f"  - {item['title']} ({item['count']})")


def save_report_json(report: dict, report_path: str) -> None:
    """Save report JSON artifact."""
    with open(report_path, "w", encoding="utf-8") as file_obj:
        json.dump(report, file_obj, ensure_ascii=False, indent=2)


def save_unmatched_titles_csv(unmatched_titles: Counter[str], path: str) -> None:
    """Save unmatched titles artifact."""
    with open(path, "w", encoding="utf-8", newline="") as file_obj:
        writer = csv.writer(file_obj)
        writer.writerow(["title", "count"])
        for title, count in _sorted_unmatched(unmatched_titles):
            writer.writerow([title, count])


def _build_unmatched_block(unmatched_items: list[dict]) -> list[str]:
    if not unmatched_items:
        return ["Unmatched Titles: None"]

    lines = ["Unmatched Titles (Top):"]
    for item in unmatched_items:
        lines.append(f"- {item['title']} ({item['count']})")
    return lines


def _render_discord_message(report: dict, unmatched_items: list[dict], fallback_note: str | None) -> str:
    lines = [
        "AC Score CSV Import Report",
        f"CSV File: {Path(report['source_csv_file']).name}",
        f"Total Songs: {report['total_song_rows']}",
        f"Matched Songs: {report['matched_song_rows']}",
        f"Unmatched Songs: {report['unmatched_song_rows']}",
        f"Match Rate: {report['match_rate']:.2f}%",
    ]

    if fallback_note is None:
        lines.extend(_build_unmatched_block(unmatched_items))
    else:
        lines.append(fallback_note)

    return "\n".join(lines)


def build_discord_import_message(report: dict, limit: int = DISCORD_SAFE_LIMIT) -> str:
    """
    Build Discord message with fallback:
    1) top 10 unmatched
    2) top 5 unmatched
    3) omit unmatched list
    """
    unmatched_top = list(report.get("unmatched_titles_topN", []))
    content = _render_discord_message(report, unmatched_top[:UNMATCHED_TOP_N], fallback_note=None)
    if len(content) <= limit:
        return content

    content = _render_discord_message(report, unmatched_top[:5], fallback_note=None)
    if len(content) <= limit:
        return content

    return _render_discord_message(report, [], fallback_note="Unmatched Titles: See log")


def send_discord_import_notification(webhook_url: str | None, content: str) -> None:
    """Send Discord webhook message. Failures are logged as warnings."""
    if not webhook_url:
        LOGGER.warning("DISCORD_WEBHOOK_URL is not set; skipping import notification")
        return

    try:
        response = requests.post(webhook_url, json={"content": content}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("Failed to send Discord import notification: %s", exc)


def resolve_discord_webhook_url(settings_path: str = "settings.yaml") -> str | None:
    """Resolve webhook URL from env first, then settings file."""
    env_value = os.environ.get("DISCORD_WEBHOOK_URL")
    if env_value and env_value.strip():
        return env_value.strip()

    settings_file = Path(settings_path)
    if not settings_file.exists():
        return None

    try:
        with settings_file.open("r", encoding="utf-8") as file_obj:
            settings = yaml.safe_load(file_obj) or {}
    except (OSError, yaml.YAMLError) as exc:
        LOGGER.warning("Failed to load settings file for webhook URL: %s", exc)
        return None

    direct = settings.get("discord_webhook_url")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    discord_section = settings.get("discord", {})
    if isinstance(discord_section, dict):
        nested = discord_section.get("webhook_url")
        if isinstance(nested, str) and nested.strip():
            return nested.strip()

    return None


def import_ac_score_csv(
    sqlite_path: str,
    csv_path: str,
    report_path: str = "import_report.json",
    unmatched_csv_path: str = "unmatched_titles.csv",
    webhook_url: str | None = None,
    settings_path: str = "settings.yaml",
    send_discord: bool = True,
) -> dict:
    """
    Import AC score CSV for title identification reporting.
    This function does not fail on Discord notification errors.
    """
    conn = sqlite3.connect(sqlite_path)
    try:
        alias_map = load_ac_alias_map(conn)
    finally:
        conn.close()

    total_song_rows, matched_song_rows, unmatched_titles = _read_csv_and_identify(csv_path, alias_map)
    report = generate_import_report(
        source_csv_file=csv_path,
        total_song_rows=total_song_rows,
        matched_song_rows=matched_song_rows,
        unmatched_titles=unmatched_titles,
    )

    save_report_json(report, report_path)
    save_unmatched_titles_csv(unmatched_titles, unmatched_csv_path)
    print_report_summary(report)

    if send_discord:
        webhook = webhook_url if webhook_url is not None else resolve_discord_webhook_url(settings_path)
        content = build_discord_import_message(report)
        send_discord_import_notification(webhook, content)

    return report


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import AC score CSV and emit alias identification report.")
    parser.add_argument("csv_path", help="Path to AC score CSV file")
    parser.add_argument("--sqlite-path", default="song_master.sqlite", help="Path to sqlite DB")
    parser.add_argument("--report-path", default="import_report.json", help="Path to output JSON report")
    parser.add_argument(
        "--unmatched-csv-path",
        default="unmatched_titles.csv",
        help="Path to output unmatched titles CSV",
    )
    parser.add_argument(
        "--settings-path",
        default="settings.yaml",
        help="Path to settings file used when DISCORD_WEBHOOK_URL is not set",
    )
    parser.add_argument("--webhook-url", default=None, help="Override Discord webhook URL")
    parser.add_argument(
        "--no-discord",
        action="store_true",
        help="Skip Discord notification",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = _build_cli_parser()
    args = parser.parse_args(argv)

    import_ac_score_csv(
        sqlite_path=args.sqlite_path,
        csv_path=args.csv_path,
        report_path=args.report_path,
        unmatched_csv_path=args.unmatched_csv_path,
        webhook_url=args.webhook_url,
        settings_path=args.settings_path,
        send_discord=not args.no_discord,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
