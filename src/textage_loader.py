"""Textageから楽曲データを取得するモジュール。"""
import json
import re
import requests


TITLE_URL = "https://textage.cc/score/titletbl.js"
DATA_URL = "https://textage.cc/score/datatbl.js"
ACT_URL = "https://textage.cc/score/actbl.js"


def _extract_js_object(js_text: str, varname: str) -> dict:
    """
    JavaScriptのテキストから指定された変数のオブジェクトを抽出し、JSON形式に変換して辞書として返す。
    JavaScriptのオブジェクト表記をJSONに変換する際に、以下の処理を行う:
    - シングルクォートをダブルクォートに変換
    - 配列内の裸の16進数値(A-F)をダブルクォートで囲む
    - JavaScriptのコメントとメソッド呼び出しを削除
    Args:
        js_text (str): JavaScriptのソースコード全体
        varname (str): 抽出対象のオブジェクト変数名
    Returns:
        dict: パースされたオブジェクトを辞書形式で返す
    Raises:
        RuntimeError: 指定された変数が見つからない場合、またはJSONのパースに失敗した場合
    """

    m = re.search(rf"{varname}\s*=\s*\{{", js_text)
    if not m:
        raise RuntimeError(f"{varname} not found in js")

    start = m.start()
    # varname= の直後の '{' の位置を特定し、対応する '}' を見つけてブロックを抜き出す
    brace_start = js_text.find('{', start)
    if brace_start == -1:
        raise RuntimeError(f"cannot find opening brace for {varname}")

    # バランスを取りつつ終端 '}' を探す（文字列リテラル中は無視）
    i = brace_start
    depth = 0
    in_str = False
    esc = False
    str_char = None
    end_idx = None
    while i < len(js_text):
        ch = js_text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == str_char:
                in_str = False
        else:
            if ch == '"' or ch == "'":
                in_str = True
                str_char = ch
            elif ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    end_idx = i
                    break
        i += 1

    if end_idx is None:
        raise RuntimeError(f"cannot find closing brace for {varname}")

    obj_text = js_text[brace_start:end_idx+1]

    # JS -> JSON化
    # 1) ファイル先頭に定義される定数 (例: SS=35) を抽出してオブジェクト内で置換
    consts = dict(re.findall(r"([A-Z_][A-Z0-9_]*)\s*=\s*([0-9]+)\s*;", js_text))

    # SS のような定数は将来の通常バージョン番号と衝突する可能性があるため、
    # 正の値ではなく負数に変換して扱う
    if consts:
        for name, val in consts.items():
            neg_val = f"-{val}"
            obj_text = re.sub(rf"(?<![\"'])\b{name}\b(?![\"'])", neg_val, obj_text)

    # 2) JavaScriptのコメント（//...）を削除
    obj_text = re.sub(r"//[^\n]*\n", "\n", obj_text)

    # 3) .fontcolor(...) などのメソッド呼び出しを削除
    obj_text = re.sub(r'\.fontcolor\([^)]*\)', '', obj_text)

    # 4) キーのシングルクォートをダブルクォートに置き換え: 'key': -> "key":
    obj_text = re.sub(r"'([^']*?)'(\s*):", r'"\1"\2:', obj_text)

    # 5) actbl.js 用の裸の16進識別子を文字列化
    obj_text = re.sub(r"(?<=,)([A-F])(?=,)", r'"\1"', obj_text)
    obj_text = re.sub(r"(?<=\[)([A-F])(?=,)", r'"\1"', obj_text)
    obj_text = re.sub(r"(?<=,)([A-F])(?=\])", r'"\1"', obj_text)

    # 6) 文字列リテラル内の制御文字（ord < 0x20）を Unicode エスケープで置換
    def _escape_ctrl(match):
        s = match.group(1)
        out = []
        i = 0
        while i < len(s):
            ch = s[i]
            if ch == '\\' and i + 1 < len(s):
                # 既存のエスケープシーケンスはそのまま保持
                out.append(ch)
                i += 1
                out.append(s[i])
            else:
                if ord(ch) < 0x20:
                    out.append('\\u%04x' % ord(ch))
                else:
                    out.append(ch)
            i += 1
        return '"' + ''.join(out) + '"'

    # titletbl のみ特別な処理（定数置換後にエントリを個別に抽出・パース）
    if varname == "titletbl":
        res = {}

        # 定数を負数に置換（SS=35 → -35）
        for name, val in consts.items():
            neg_val = f"-{val}"
            obj_text = re.sub(rf"(?<![\"'])\b{name}\b(?![\"'])", neg_val, obj_text)

        entry_re = re.compile(r"['\"]([^'\"]+)['\"]\s*:\s*(\[[^\]]*\])", flags=re.S)
        for key, arr_text in entry_re.findall(obj_text):
            try:
                arr = json.loads(arr_text)

                # arr[0] は version のため、文字列として格納する
                if isinstance(arr, list) and len(arr) > 0:
                    arr[0] = str(arr[0])

                res[key] = arr
            except json.JSONDecodeError:
                continue

        return res

    # その他のテーブル（datatbl, actbl）は元の処理で対応
    obj_text = re.sub(r'"((?:\\.|[^"\\\n])*)"', _escape_ctrl, obj_text)

    try:
        parsed = json.loads(obj_text)
        return parsed
    except Exception as e:
        raise RuntimeError(f"json parse failed for {varname}: {e}") from e


def fetch_textage_tables() -> tuple[dict, dict, dict]:
    """
    Textageからゲーム曲のマスターデータを取得する関数
    3つの外部URLからHTTP GETリクエストを実行し、
    JavaScript オブジェクト形式で記述されたテーブルデータを抽出して返す。
    Returns:
        tuple[dict, dict, dict]: 以下の3つの辞書を含むタプル
            - titletbl (dict): 曲のタイトル情報を格納した辞書
            - datatbl (dict): 曲のスコアデータ情報を格納した辞書
            - actbl (dict): 設定活動データを格納した辞書
    Raises:
        requests.exceptions.HTTPError: HTTP リクエストが失敗した場合
        requests.exceptions.Timeout: リクエストがタイムアウトした場合
    """
    r1 = requests.get(TITLE_URL, timeout=30)
    r1.raise_for_status()

    r2 = requests.get(DATA_URL, timeout=30)
    r2.raise_for_status()

    r3 = requests.get(ACT_URL, timeout=30)
    r3.raise_for_status()

    titletbl = _extract_js_object(r1.text, "titletbl")
    datatbl = _extract_js_object(r2.text, "datatbl")
    actbl = _extract_js_object(r3.text, "actbl")

    return titletbl, datatbl, actbl
