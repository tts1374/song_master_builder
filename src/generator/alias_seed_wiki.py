"""Seed csv_wiki aliases based on parsed BEMANIWiki conversion rows."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from src.wiki.bemaniwiki_parse_title_alias import WikiAliasRow

ALIAS_TYPE_CSV_WIKI = "csv_wiki"
ALIAS_SCOPE_AC = "ac"


@dataclass(frozen=True)
class WikiAliasSeedReport:
    """Insertion report for csv_wiki aliases."""

    resolved_rows: int
    unresolved_official_titles: tuple[str, ...]
    inserted_csv_wiki_alias_count: int
    dedup_skipped_count: int
    max_csv_wiki_candidates_per_song: int


def seed_wiki_aliases(
    conn: sqlite3.Connection,
    wiki_rows: list[WikiAliasRow],
    now_utc_iso: str,
    unresolved_official_title_fail_threshold: int | None = None,
) -> WikiAliasSeedReport:
    """Resolve official titles and insert wiki-replaced aliases."""
    cur = conn.cursor()

    unresolved_titles: list[str] = []
    resolved_rows = 0
    inserted_count = 0
    dedup_skipped_count = 0
    inserted_pairs: set[tuple[str, str, str]] = set()
    per_song_inserted_count: dict[str, int] = {}

    for row in wiki_rows:
        cur.execute(
            """
            SELECT textage_id
            FROM music
            WHERE title = ?
              AND is_ac_active = 1
            LIMIT 2;
            """,
            (row.official_title,),
        )
        matched = cur.fetchall()

        if len(matched) == 0:
            unresolved_titles.append(row.official_title)
            continue
        if len(matched) > 1:
            raise RuntimeError(
                "music.title is not unique for wiki alias resolution: "
                f"{row.official_title}"
            )

        resolved_rows += 1
        textage_id = str(matched[0][0])
        deduped_titles = tuple(dict.fromkeys(row.replaced_titles))
        for replaced_title in deduped_titles:
            if replaced_title == row.official_title:
                dedup_skipped_count += 1
                continue

            pair = (ALIAS_SCOPE_AC, textage_id, replaced_title)
            if pair in inserted_pairs:
                dedup_skipped_count += 1
                continue

            try:
                cur.execute(
                    """
                    INSERT INTO music_title_alias (
                        alias_scope, textage_id, alias, alias_type, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ALIAS_SCOPE_AC,
                        textage_id,
                        replaced_title,
                        ALIAS_TYPE_CSV_WIKI,
                        now_utc_iso,
                        now_utc_iso,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise RuntimeError(
                    "alias collision detected (scope unique alias violated): "
                    f"{ALIAS_SCOPE_AC}:{replaced_title}"
                ) from exc

            inserted_pairs.add(pair)
            inserted_count += 1
            per_song_inserted_count[textage_id] = per_song_inserted_count.get(textage_id, 0) + 1

    if (
        unresolved_official_title_fail_threshold is not None
        and len(unresolved_titles) > unresolved_official_title_fail_threshold
    ):
        raise RuntimeError(
            "too many unresolved official titles in wiki alias import: "
            f"{len(unresolved_titles)} > {unresolved_official_title_fail_threshold}"
        )

    return WikiAliasSeedReport(
        resolved_rows=resolved_rows,
        unresolved_official_titles=tuple(unresolved_titles),
        inserted_csv_wiki_alias_count=inserted_count,
        dedup_skipped_count=dedup_skipped_count,
        max_csv_wiki_candidates_per_song=max(per_song_inserted_count.values(), default=0),
    )
