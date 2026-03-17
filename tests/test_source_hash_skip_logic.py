"""Tests for build source hash skip decision."""

from __future__ import annotations

import pytest

from main import (
    INF_PACK_HASH_KEY,
    LEGACY_MANUAL_ALIAS_HASH_KEY,
    MANUAL_ALIAS_AC_HASH_KEY,
    MANUAL_ALIAS_INF_HASH_KEY,
    has_same_textage_source_hashes,
)


@pytest.mark.light
def test_has_same_textage_source_hashes_true_when_all_required_hashes_match():
    previous = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "f",
    }
    current = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "f",
    }
    assert has_same_textage_source_hashes(previous, current) is True


@pytest.mark.light
def test_has_same_textage_source_hashes_false_when_inf_hash_is_missing_in_previous():
    previous = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        INF_PACK_HASH_KEY: "f",
    }
    current = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "f",
    }
    assert has_same_textage_source_hashes(previous, current) is False


@pytest.mark.light
def test_has_same_textage_source_hashes_false_when_ac_hash_differs():
    previous = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "old",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "f",
    }
    current = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "new",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "f",
    }
    assert has_same_textage_source_hashes(previous, current) is False


@pytest.mark.light
def test_has_same_textage_source_hashes_false_when_inf_hash_differs():
    previous = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "old",
        INF_PACK_HASH_KEY: "f",
    }
    current = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "new",
        INF_PACK_HASH_KEY: "f",
    }
    assert has_same_textage_source_hashes(previous, current) is False


@pytest.mark.light
def test_has_same_textage_source_hashes_false_when_inf_pack_hash_differs():
    previous = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "old",
    }
    current = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "new",
    }
    assert has_same_textage_source_hashes(previous, current) is False


@pytest.mark.light
def test_has_same_textage_source_hashes_false_when_previous_hashes_is_none():
    current = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "f",
    }
    assert has_same_textage_source_hashes(None, current) is False


@pytest.mark.light
def test_has_same_textage_source_hashes_accepts_legacy_manual_alias_key_for_ac():
    previous = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        LEGACY_MANUAL_ALIAS_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "f",
    }
    current = {
        "titletbl.js": "a",
        "datatbl.js": "b",
        "actbl.js": "c",
        MANUAL_ALIAS_AC_HASH_KEY: "d",
        MANUAL_ALIAS_INF_HASH_KEY: "e",
        INF_PACK_HASH_KEY: "f",
    }
    assert has_same_textage_source_hashes(previous, current) is True
