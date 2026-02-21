"""Load Textage JS tables and parse them into Python dicts."""

from __future__ import annotations

import hashlib
import json
import re

import requests

TITLE_URL = "https://textage.cc/score/titletbl.js"
DATA_URL = "https://textage.cc/score/datatbl.js"
ACT_URL = "https://textage.cc/score/actbl.js"
CONTENT_TYPE_CHARSET_RE = re.compile(r"charset\s*=\s*([A-Za-z0-9._-]+)", flags=re.I)


# pylint: disable-next=too-many-locals,too-many-branches,too-many-statements
def _extract_js_object(js_text: str, varname: str) -> dict:
    """
    Extract and parse `varname = {...}` object from JS source text.

    Preprocess steps:
    - Replace constants (e.g., `SS=35`) with negative value convention used in this project.
    - Strip line comments.
    - Strip `.fontcolor(...)` decorations.
    - Convert single-quoted object keys to JSON-compatible double quotes.
    - Convert actbl's bare A-F tokens into quoted strings.
    """
    match = re.search(rf"{varname}\s*=\s*\{{", js_text)
    if not match:
        raise RuntimeError(f"{varname} not found in JS")

    start = match.start()
    brace_start = js_text.find("{", start)
    if brace_start == -1:
        raise RuntimeError(f"opening brace for {varname} not found")

    index = brace_start
    depth = 0
    in_str = False
    escaped = False
    str_char = ""
    end_index = None
    while index < len(js_text):
        ch = js_text[index]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == str_char:
                in_str = False
        else:
            if ch in ('"', "'"):
                in_str = True
                str_char = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end_index = index
                    break
        index += 1

    if end_index is None:
        raise RuntimeError(f"closing brace for {varname} not found")

    obj_text = js_text[brace_start : end_index + 1]

    consts = dict(re.findall(r"([A-Z_][A-Z0-9_]*)\s*=\s*([0-9]+)\s*;", js_text))
    for name, val in consts.items():
        obj_text = re.sub(rf"(?<![\"'])\b{name}\b(?![\"'])", f"-{val}", obj_text)

    obj_text = re.sub(r"//[^\n]*(?=\n|$)", "", obj_text)
    obj_text = re.sub(r"\.fontcolor\([^)]*\)", "", obj_text)
    obj_text = re.sub(r"'([^']*?)'(\s*):", r'"\1"\2:', obj_text)

    obj_text = re.sub(r"(?<=,)([A-F])(?=,)", r'"\1"', obj_text)
    obj_text = re.sub(r"(?<=\[)([A-F])(?=,)", r'"\1"', obj_text)
    obj_text = re.sub(r"(?<=,)([A-F])(?=\])", r'"\1"', obj_text)

    def _escape_ctrl(match_obj: re.Match[str]) -> str:
        """Escape raw control characters inside JSON-like string literals."""
        src = match_obj.group(1)
        out: list[str] = []
        idx = 0
        while idx < len(src):
            ch = src[idx]
            if ch == "\\" and idx + 1 < len(src):
                out.append(ch)
                idx += 1
                out.append(src[idx])
            else:
                if ord(ch) < 0x20:
                    out.append(f"\\u{ord(ch):04x}")
                else:
                    out.append(ch)
            idx += 1
        return '"' + "".join(out) + '"'

    if varname == "titletbl":
        result: dict[str, list] = {}
        entry_re = re.compile(r"['\"]([^'\"]+)['\"]\s*:\s*(\[[^\]]*\])", flags=re.S)
        for key, arr_text in entry_re.findall(obj_text):
            try:
                arr = json.loads(arr_text)
            except json.JSONDecodeError:
                continue

            if isinstance(arr, list) and arr:
                arr[0] = str(arr[0])
            result[key] = arr
        return result

    obj_text = re.sub(r'"((?:\\.|[^"\\\n])*)"', _escape_ctrl, obj_text)
    try:
        return json.loads(obj_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"json parse failed for {varname}: {exc}") from exc


def _sha256_hex(data: bytes) -> str:
    """Return SHA-256 hex digest for raw bytes."""
    return hashlib.sha256(data).hexdigest()


def _charset_from_content_type(content_type: str | None) -> str | None:
    """Extract charset token from Content-Type header."""
    if not content_type:
        return None
    match = CONTENT_TYPE_CHARSET_RE.search(content_type)
    if not match:
        return None
    return match.group(1).strip()


def _decode_textage_response(response: requests.Response) -> str:
    """
    Decode Textage JS bytes deterministically.

    Textage endpoints usually omit charset, and requests' guess can be wrong for Japanese text.
    """
    raw = response.content
    candidates: list[str] = []

    header_charset = _charset_from_content_type(response.headers.get("Content-Type"))
    if header_charset:
        candidates.append(header_charset)
    if response.encoding:
        candidates.append(response.encoding)

    for encoding in ("cp932", "shift_jis", "utf-8", "euc_jp"):
        candidates.append(encoding)

    seen: set[str] = set()
    ordered_candidates = []
    for candidate in candidates:
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered_candidates.append(candidate)

    for encoding in ordered_candidates:
        try:
            return raw.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue

    return raw.decode("cp932", errors="replace")


def fetch_textage_tables_with_hashes() -> tuple[dict, dict, dict, dict[str, str]]:
    """Fetch Textage titletbl/datatbl/actbl and return parsed tables with source hashes."""
    title_resp = requests.get(TITLE_URL, timeout=30)
    title_resp.raise_for_status()

    data_resp = requests.get(DATA_URL, timeout=30)
    data_resp.raise_for_status()

    act_resp = requests.get(ACT_URL, timeout=30)
    act_resp.raise_for_status()

    title_text = _decode_textage_response(title_resp)
    data_text = _decode_textage_response(data_resp)
    act_text = _decode_textage_response(act_resp)

    titletbl = _extract_js_object(title_text, "titletbl")
    datatbl = _extract_js_object(data_text, "datatbl")
    actbl = _extract_js_object(act_text, "actbl")

    source_hashes = {
        "titletbl.js": _sha256_hex(title_resp.content),
        "datatbl.js": _sha256_hex(data_resp.content),
        "actbl.js": _sha256_hex(act_resp.content),
    }

    return titletbl, datatbl, actbl, source_hashes


def fetch_textage_tables() -> tuple[dict, dict, dict]:
    """Fetch Textage tables and return parsed table dicts."""
    titletbl, datatbl, actbl, _ = fetch_textage_tables_with_hashes()
    return titletbl, datatbl, actbl
