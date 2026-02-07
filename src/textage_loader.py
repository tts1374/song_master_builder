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
    body = js_text[start:]

    # varname= の直後から最後の "};" までを抜く
    m2 = re.search(rf"{varname}\s*=\s*(\{{.*\}})\s*;", body, flags=re.S)
    if not m2:
        raise RuntimeError(f"cannot parse {varname} body")

    obj_text = m2.group(1)

    # JS -> JSON化
    # 1. JavaScriptのコメント（//...）を削除
    obj_text = re.sub(r"//[^\n]*\n", "\n", obj_text)

    # 2. .fontcolor(...) などのメソッド呼び出しを削除
    obj_text = re.sub(r'\.fontcolor\([^)]*\)', '', obj_text)

    # 3. キーのシングルクォートを置き換え
    # パターン: 'key': を "key": に（キー内にエスケープされたクォートを含むかもしれない）
    def replace_key(match):
        key = match.group(1)
        # キー内のエスケープされたシングルクォート（\'）をダブルクォートに置き換え
        key = key.replace("\\'", '"')
        return f'"{key}":'

    obj_text = re.sub(r"'((?:[^'\\]|\\.)*)'(\s*):", replace_key, obj_text)

    # 4. 残っているシングルクォートをダブルクォートに置き換え
    # （値内のシングルクォート）
    obj_text = re.sub(r"\'", '"', obj_text)

    # 5. actbl.js は A,B,C,F などが裸で入っているので "A" に変換
    # 例: [3,0,0,3,1,6,7,A,7,C,...]
    obj_text = re.sub(r"(?<=,)([A-F])(?=,)", r'"\1"', obj_text)
    obj_text = re.sub(r"(?<=\[)([A-F])(?=,)", r'"\1"', obj_text)
    obj_text = re.sub(r"(?<=,)([A-F])(?=\])", r'"\1"', obj_text)

    # JSONとしてロード
    try:
        return json.loads(obj_text)
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
