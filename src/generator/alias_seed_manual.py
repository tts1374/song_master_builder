"""Seed manual aliases from a repository-managed CSV file."""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path

ALIAS_TYPE_MANUAL = "manual"
ALIAS_TYPE_OFFICIAL = "official"
ALLOWED_ALIAS_SCOPES = {"ac", "inf"}
REQUIRED_COLUMNS = ("textage_id", "alias", "alias_scope", "alias_type")


@dataclass(frozen=True)
class ManualAliasCsvRow:
    """One validated row from manual alias CSV."""

    line_number: int
    textage_id: str
    alias: str
    alias_scope: str
    alias_type: str
    note: str


@dataclass(frozen=True)
class ManualAliasSeedReport:
    """Insertion report for manual aliases."""

    inserted_manual_alias_count: int
    skipped_redundant_manual_alias_count: int


def _read_manual_alias_csv(csv_path: str | Path) -> list[ManualAliasCsvRow]:
    path = Path(csv_path)
    if not path.exists():
        raise RuntimeError(f"manual alias CSV not found: {path}")

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
            reader = csv.DictReader(file_obj)
            fieldnames = tuple(reader.fieldnames or ())
            missing_columns = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
            if missing_columns:
                raise RuntimeError(
                    "manual alias CSV missing required columns: "
                    f"{', '.join(missing_columns)}"
                )

            rows: list[ManualAliasCsvRow] = []
            for line_number, row in enumerate(reader, start=2):
                textage_id = str(row.get("textage_id", "")).strip()
                alias = str(row.get("alias", "")).strip()
                alias_scope = str(row.get("alias_scope", "")).strip()
                alias_type = str(row.get("alias_type", "")).strip()
                note = str(row.get("note", "")).strip()

                if not textage_id:
                    raise RuntimeError(
                        "manual alias CSV has empty required value: "
                        f"textage_id (line={line_number})"
                    )
                if not alias:
                    raise RuntimeError(
                        "manual alias CSV has empty required value: "
                        f"alias (line={line_number})"
                    )
                if not alias_scope:
                    raise RuntimeError(
                        "manual alias CSV has empty required value: "
                        f"alias_scope (line={line_number})"
                    )
                if not alias_type:
                    raise RuntimeError(
                        "manual alias CSV has empty required value: "
                        f"alias_type (line={line_number})"
                    )
                if alias_scope not in ALLOWED_ALIAS_SCOPES:
                    raise RuntimeError(
                        "manual alias CSV has invalid alias_scope "
                        f"(line={line_number}, value={alias_scope!r})"
                    )
                if alias_type != ALIAS_TYPE_MANUAL:
                    raise RuntimeError(
                        "manual alias CSV has invalid alias_type "
                        f"(line={line_number}, value={alias_type!r})"
                    )

                rows.append(
                    ManualAliasCsvRow(
                        line_number=line_number,
                        textage_id=textage_id,
                        alias=alias,
                        alias_scope=alias_scope,
                        alias_type=alias_type,
                        note=note,
                    )
                )
    except RuntimeError:
        raise
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise RuntimeError(f"failed to read manual alias CSV: {path}") from exc

    return rows


def _validate_no_duplicate_scope_alias(rows: list[ManualAliasCsvRow]) -> None:
    first_seen_at: dict[tuple[str, str], int] = {}
    duplicates: list[tuple[tuple[str, str], int, int]] = []

    for row in rows:
        key = (row.alias_scope, row.alias)
        first_line = first_seen_at.get(key)
        if first_line is None:
            first_seen_at[key] = row.line_number
            continue
        duplicates.append((key, first_line, row.line_number))

    if duplicates:
        sample = "; ".join(
            f"{scope}:{alias!r} first_line={first_line} dup_line={dup_line}"
            for (scope, alias), first_line, dup_line in duplicates[:10]
        )
        raise RuntimeError(
            "manual alias CSV has duplicate (alias_scope, alias) rows "
            f"(count={len(duplicates)}): {sample}"
        )


def _validate_textage_ids_exist(conn: sqlite3.Connection, rows: list[ManualAliasCsvRow]) -> None:
    cur = conn.cursor()
    cur.execute("SELECT textage_id FROM music;")
    existing_textage_ids = {str(value[0]) for value in cur.fetchall()}

    missing_rows = [row for row in rows if row.textage_id not in existing_textage_ids]
    if missing_rows:
        sample = "; ".join(
            f"line={row.line_number} textage_id={row.textage_id!r}"
            for row in missing_rows[:10]
        )
        raise RuntimeError(
            "manual alias CSV has textage_id not found in music "
            f"(count={len(missing_rows)}): {sample}"
        )


def _load_official_alias_triples(conn: sqlite3.Connection) -> set[tuple[str, str, str]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT textage_id, alias_scope, alias
        FROM music_title_alias
        WHERE alias_type = ?;
        """,
        (ALIAS_TYPE_OFFICIAL,),
    )
    return {(str(row[0]), str(row[1]), str(row[2])) for row in cur.fetchall()}


def seed_manual_aliases_from_csv(
    conn: sqlite3.Connection,
    csv_path: str | Path,
    now_utc_iso: str,
) -> ManualAliasSeedReport:
    """
    Load manual aliases from CSV and insert them into music_title_alias.

    Validation failures abort before insertion.
    Redundant rows that exactly duplicate an official alias are skipped with warning.
    """
    rows = _read_manual_alias_csv(csv_path)
    _validate_no_duplicate_scope_alias(rows)
    _validate_textage_ids_exist(conn, rows)

    cur = conn.cursor()
    official_aliases = _load_official_alias_triples(conn)

    inserted_count = 0
    skipped_redundant_count = 0

    for row in rows:
        triple = (row.textage_id, row.alias_scope, row.alias)
        if triple in official_aliases:
            skipped_redundant_count += 1
            print(
                "[alias/manual] warning: redundant row skipped "
                f"(line={row.line_number}, textage_id={row.textage_id}, "
                f"scope={row.alias_scope}, alias={row.alias!r})"
            )
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
                    row.alias_scope,
                    row.textage_id,
                    row.alias,
                    ALIAS_TYPE_MANUAL,
                    now_utc_iso,
                    now_utc_iso,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise RuntimeError(
                "manual alias collision detected "
                "(music_title_alias UNIQUE(alias_scope, alias) violated): "
                f"line={row.line_number}, scope={row.alias_scope}, alias={row.alias!r}"
            ) from exc

        inserted_count += 1

    return ManualAliasSeedReport(
        inserted_manual_alias_count=inserted_count,
        skipped_redundant_manual_alias_count=skipped_redundant_count,
    )

